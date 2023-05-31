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
import _ from 'lodash'
// eslint-disable-next-line camelcase
import { encoding_for_model, TiktokenModel } from '@dqbd/tiktoken'
import { detailedCompare, invertNaclCase, walkOnElement, WALK_NEXT_STEP } from '@salto-io/adapter-utils'
import { Change, ChangeDataType, DetailedChange, getChangeData, isAdditionChange, isElement, isInstanceElement, isModificationChange, isObjectType, isRemovalChange, ModificationChange, Value } from '@salto-io/adapter-api'
import { logger } from '@salto-io/logging'
import { values } from '@salto-io/lowerdash'
import { parser, nacl, staticFiles } from '@salto-io/workspace'
import { chunkBySoft } from './utils'

const log = logger(module)

// TODO: once we have aliases, use aliases
const getReadableName = (elem: ChangeDataType): string => {
  const typeName = invertNaclCase(elem.elemID.typeName)
  if (isInstanceElement(elem)) {
    // const instName = (
    //   elem.annotations[INSTANCE_ANNOTATIONS.ALIAS]
    //   || elem.value.name
    //   || elem.value.Name
    //   || invertNaclCase(elem.elemID.name)
    // )
    return `"${typeName}.${invertNaclCase(elem.elemID.name)}"`
  }
  return typeName
}

const dumpNacl = async (value: Value): Promise<string> => {
  const dummyStaticFileSource = staticFiles.buildInMemStaticFilesSource()
  const functions = nacl.getFunctions(dummyStaticFileSource)

  let newData: string
  if (isElement(value)) {
    if (isObjectType(value)) {
      const clone = value.clone()
      clone.annotationRefTypes = {}
      newData = await parser.dumpElements([clone], functions, 0)
    } else {
      newData = await parser.dumpElements([value], functions, 0)
    }
  } else {
    newData = await parser.dumpValues(value, functions, 0)
  }

  return newData.trimStart().trimEnd()
}

const indent = (text: string, level: number, opts: { prefix: string } = { prefix: '' }): string => {
  const indentText = _.repeat('  ', level)
  return text.split('\n').map(line => `${opts.prefix}${indentText}${line}`).join('\n')
}


type EncodingLen = (text: string) => number
const getReadableDescriptionOfDetailedChange = async (
  detailedChange: DetailedChange,
  maxValueLength: number,
  encodingLength: EncodingLen,
): Promise<string | undefined> => {
  const pathInBaseID = detailedChange.id.createBaseID().path
  const changeKey = pathInBaseID.join('.') || getReadableName(getChangeData(detailedChange))
  if (isAdditionChange(detailedChange)) {
    const dumpedValue = await dumpNacl(detailedChange.data.after)
    const fullChangeString = isElement(detailedChange.data.after)
      ? `Added:\n${indent(dumpedValue, 1)}`
      : `Added: ${indent(`${changeKey} = ${dumpedValue}`, 1)}`
    if (encodingLength(fullChangeString) < maxValueLength) {
      return fullChangeString
    }
    return `Added ${changeKey}`
  }
  if (isRemovalChange(detailedChange)) {
    const dumpedValue = await dumpNacl(detailedChange.data.before)
    const fullChangeString = isElement(detailedChange.data.before)
      ? `Removed:\n${indent(dumpedValue, 1)}`
      : `Removed: ${indent(`${changeKey} = ${dumpedValue}`, 1)}`
    if (encodingLength(fullChangeString) < maxValueLength) {
      return fullChangeString
    }
    return `Removed ${changeKey}`
  }
  if (isModificationChange(detailedChange)) {
    const dumpedBefore = await dumpNacl(detailedChange.data.before)
    const dumpedAfter = await dumpNacl(detailedChange.data.after)

    if (dumpedBefore === dumpedAfter) {
      // This is just an index change, no need to dump the value
      // TODO: sometimes index changes are meaningful, need to figure out a way to summarize those
      return undefined
    }

    const fullChangeString = dumpedBefore.includes('\n') || dumpedAfter.includes('\n')
      ? `Changed ${changeKey} from:\n${indent(dumpedBefore, 1)}\n\nto:\n${indent(dumpedAfter, 1)}`
      : `Changed ${changeKey} from ${dumpedBefore} to ${dumpedAfter}`
    if (encodingLength(fullChangeString) < maxValueLength) {
      return fullChangeString
    }
    return `Changed value of ${changeKey}`
  }
  // Should never happen
  throw new Error('Unrecognized change type')
}


const IMPORTANT_ATTRS: Record<string, string[]> = {
  jira: ['name', 'key', 'state', 'enabled', 'projectTypeKey'],
  salesforce: ['name', 'label', 'apiName', 'fullName'],
}

const removeBracketLines = (dumpedObject: string): string => (
  // We remove the first line that has the opening bracket and the last line to ignore the closing bracket,
  dumpedObject.split('\n').slice(1, -1).join('\n')
)

const getReadableModificationDescription = async (
  change: ModificationChange<ChangeDataType>,
  dumpLimit: number,
  encodingLength: EncodingLen,
): Promise<string> => {
  const changedElemName = getReadableName(change.data.after)
  const detailedChanges = detailedCompare(
    change.data.before,
    change.data.after,
    { compareListItems: true, createFieldChanges: false },
  )
  // Important attributes
  const detailedChangeIDs = new Set(detailedChanges.map(detailedChange => detailedChange.id.getFullName()))
  const importantValues = {}
  walkOnElement({
    element: change.data.after,
    func: ({ value, path }) => {
      if (
        (IMPORTANT_ATTRS[path.adapter] ?? []).includes(path.createBaseID().path[0])
        && !detailedChangeIDs.has(path.getFullName())
      ) {
        _.set(importantValues, path.createBaseID().path, value)
        return WALK_NEXT_STEP.SKIP
      }
      return WALK_NEXT_STEP.RECURSE
    },
  })
  const dumpedImportant = _.isEmpty(importantValues) ? '' : removeBracketLines(await dumpNacl(importantValues)).concat('\n')

  const dumpedChanges = await Promise.all(
    detailedChanges.map(detailed => getReadableDescriptionOfDetailedChange(detailed, dumpLimit, encodingLength))
  )
  return `In ${changedElemName}:\n${dumpedImportant}${indent(dumpedChanges.filter(values.isDefined).join('\n'), 1)}`
}

type FormatChangesOptions = {
  maxTokens?: number
  modelName?: TiktokenModel
  regexToFilterOut?: string[]
}
const FORMAT_CHANGES_DEFAULT_OPTIONS = {
  maxTokens: 3500,
  modelName: 'gpt-3.5-turbo' as const,
}
export const formatChangesForPrompt = async (changes: Change[], opts?: FormatChangesOptions): Promise<string[]> => {
  const params = { ...FORMAT_CHANGES_DEFAULT_OPTIONS, ...opts }
  const encoding = encoding_for_model(params.modelName)
  const encodingLength = (text: string): number => encoding.encode(text).length


  const [removals, addOrModify] = _.partition(changes, isRemovalChange)
  const [additions, modifications] = _.partition(addOrModify, isAdditionChange)

  const formattedAdditions = await Promise.all(
    additions.map(change => getReadableDescriptionOfDetailedChange(
      { ...change, id: change.data.after.elemID },
      params.maxTokens,
      encodingLength,
    ))
  )
  const formattedRemovals = await Promise.all(
    removals.map(change => getReadableDescriptionOfDetailedChange(
      { ...change, id: change.data.before.elemID },
      params.maxTokens,
      encodingLength,
    ))
  )
  const formattedModifications = await Promise.all(
    modifications.map(change => getReadableModificationDescription(change, params.maxTokens, encodingLength))
  )
  log.debug('filtering out %o', params.regexToFilterOut)
  const formattedChanges = formattedAdditions
    .concat(formattedRemovals)
    .concat(formattedModifications)
    .filter(values.isDefined)
    .map(changesStr => (params.regexToFilterOut ?? []).reduce((str, regex) => str.replace(new RegExp(regex, 'm'), ''), changesStr))

  const changeChunks = chunkBySoft(
    formattedChanges,
    params.maxTokens,
    formattedChange => encodingLength(formattedChange),
  )

  const formattedChunks = changeChunks.map(chunk => chunk.join('\n\n'))
  log.debug('Formatted changes token lengths: %s', formattedChunks.map(encodingLength).join('\n'))
  return formattedChunks
}
