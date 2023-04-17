/*
*                      Copyright 2023 Salto Labs Ltd.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import { logger } from '@salto-io/logging'
import { collections, promises } from '@salto-io/lowerdash'
import { ElemID, ElemIDType, Field, isObjectType, ReadOnlyElementsSource, ReferenceExpression } from '@salto-io/adapter-api'
import { extendGeneratedDependencies, naclCase } from '@salto-io/adapter-utils'
import { LocalFilterCreator } from '../filter'
import { isFormulaField } from '../transformers/transformer'
import { CUSTOM_METADATA_SUFFIX, FORMULA, SALESFORCE } from '../constants'
import { FormulaIdentifierInfo, IdentifierType, parseFormulaIdentifier } from './formula_utils/parse'
import { buildElementsSourceForFetch, extractFlatCustomObjectFields } from './utils'

// should not use this type of import, if formulon is missing type definitions, add them
/* eslint-disable-next-line @typescript-eslint/no-var-requires */
const formulon = require('formulon')

const { extract } = formulon

const log = logger(module)
const { awu } = collections.asynciterable

const identifierTypeToElementName = (identifierInfo: FormulaIdentifierInfo): string[] => {
  if (identifierInfo.type === 'customLabel') {
    return [identifierInfo.instance]
  }
  if (identifierInfo.type === 'customMetadataTypeRecord') {
    const [typeName, instanceName] = identifierInfo.instance.split('.')
    return [`${typeName.slice(0, -1 * CUSTOM_METADATA_SUFFIX.length)}.${instanceName}`]
  }
  return identifierInfo.instance.split('.').slice(1)
}

const identifierTypeToElementType = (identifierInfo: FormulaIdentifierInfo): string => {
  if (identifierInfo.type === 'customLabel') {
    return 'CustomLabel'
  }

  return identifierInfo.instance.split('.')[0]
}

const identifierTypeToElemIdType = (identifierInfo: FormulaIdentifierInfo): ElemIDType => (
  ({
    standardObject: 'type',
    customMetadataType: 'type',
    customObject: 'type',
    customSetting: 'type',
    standardField: 'field',
    customField: 'field',
    customMetadataTypeRecord: 'instance',
    customLabel: 'instance',
  } as Record<IdentifierType, ElemIDType>)[identifierInfo.type]
)

const referencesFromIdentifiers = async (typeInfos: FormulaIdentifierInfo[]): Promise<ElemID[]> => (
  typeInfos
    .map(identifierInfo => (
      new ElemID(SALESFORCE,
        naclCase(identifierTypeToElementType(identifierInfo)),
        identifierTypeToElemIdType(identifierInfo),
        ...identifierTypeToElementName(identifierInfo).map(naclCase))
    ))
)

const addDependenciesAnnotation = async (field: Field, allElements: ReadOnlyElementsSource): Promise<void> => {
  const isValidReference = async (elemId: ElemID): Promise<boolean> => {
    if (elemId.idType === 'field') {
      // In the previous code there was an assumption that any id which is not `type` or `instance` is a field
      // but that is not true, it is not safe to make that assumption
      const typeElement = await allElements.get(elemId.createTopLevelParentID().parent)
      return (typeElement !== undefined) && (typeElement.fields[elemId.name] !== undefined)
    }

    return allElements.has(elemId)
  }

  const logInvalidReferences = (
    invalidReferences: ElemID[],
    formula: string,
    identifiersInfo: FormulaIdentifierInfo[][]
  ): void => {
    if (invalidReferences.length > 0) {
      // error level log means something is going to fail, we encountered a scenario that we cannot handle
      // in this case, we can and do handle this scenario
      // this should not be more than a "warn" (same for all other error logs here)
      log.error('When parsing the formula %o in field %o, one or more of the identifiers %o was parsed to an invalid reference: ',
        formula,
        field.elemID.getFullName(),
        identifiersInfo.flat().map(info => info.instance))
    }
    invalidReferences.forEach(refElemId => {
      log.error(`Invalid reference: ${refElemId.getFullName()}`)
    })
  }

  const formula = field.annotations[FORMULA]
  if (formula === undefined) {
    log.error(`Field ${field.elemID.getFullName()} is a formula field with no formula?`)
    return
  }

  // log per field in SF is a crazy amount of logs
  // this should at most be a trace log, but probably not needed at all
  // also, logs should use %s, not string template literals
  log.debug(`Extracting formula refs from ${field.elemID.getFullName()}`)

  try {
    // similar - this will create a crazy amount of logs
    const formulaIdentifiers: string[] = log.time(
      () => (extract(formula)),
      `Parse formula '${formula.slice(0, 15)}'`
    )

    const identifiersInfo = await log.time(
      () => Promise.all(
        // Using the typename here is not technically correct, we should be using the salesforce name
        // this should work in a similar way to field_references, creating an index of elements by API name
        // it may be possible to share some of the code?
        // this will also make some of the code below redundant
        formulaIdentifiers.map(async identifier => parseFormulaIdentifier(identifier, field.parent.elemID.typeName))
      ),
      'Convert formula identifiers to references'
    )

    // We check the # of refs before we filter bad refs out because otherwise the # of refs will be affected by the
    // filtering.
    const references = (await referencesFromIdentifiers(identifiersInfo.flat()))

    // this is essentially comparing identifiersInfo.length with identifiersInfo.flat().length, which will almost always
    // be true (unless identifiersInfo.length is 0)
    if (references.length < identifiersInfo.length) {
      log.warn(`Some formula identifiers were not converted to references.
      Field: ${field.elemID.getFullName()}
      Formula: ${formula}
      Identifiers: ${identifiersInfo.flat().map(info => info.instance).join(', ')}
      References: ${references.map(ref => ref.getFullName()).join(', ')}`)
    }

    const [validReferences, invalidReferences] = await promises.array.partition(references, isValidReference)
    logInvalidReferences(invalidReferences, formula, identifiersInfo)

    log.info(`Extracted ${validReferences.length} valid references`)
    const depsAsRefExpr = validReferences.map(elemId => ({ reference: new ReferenceExpression(elemId) }))

    extendGeneratedDependencies(field, depsAsRefExpr)
  } catch (e) {
    log.warn(`Failed to extract references from formula ${formula}: ${e}`)
  }
}

/**
 * Extract references from formulas
 * Formulas appear in the field definitions of types and may refer to fields in their parent type or in another type.
 * This filter parses formulas, identifies such references and adds them to the _generated_references annotation of the
 * formula field.
 * Note: Currently (pending a fix to SALTO-3176) we only look at formula fields in custom objects.
 */
const filter: LocalFilterCreator = ({ config }) => ({
  name: 'formula_deps',
  onFetch: async fetchedElements => {
    if (config.fetchProfile.isFeatureEnabled('skipParsingFormulas')) {
      log.info('Formula parsing is disabled. Skipping formula_deps filter.')
      return
    }
    const fetchedObjectTypes = fetchedElements.filter(isObjectType)
    const fetchedFormulaFields = await awu(fetchedObjectTypes)
      .flatMap(extractFlatCustomObjectFields) // Get the types + their fields
      .filter(isFormulaField)
      .toArray()
    const allElements = buildElementsSourceForFetch(fetchedElements, config)
    await Promise.all(fetchedFormulaFields.map(field => addDependenciesAnnotation(field, allElements)))
  },
})

export default filter
