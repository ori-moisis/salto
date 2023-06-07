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
import { Change, ChangeDataType, DetailedChange, Element, getChangeData, isAdditionChange, isElement, isModificationChange, isObjectType, isRemovalChange, ModificationChange, StaticFile, Value } from '@salto-io/adapter-api'
import { logger } from '@salto-io/logging'
import { values, collections } from '@salto-io/lowerdash'
import { parser, nacl, staticFiles } from '@salto-io/workspace'
import { chunkBySoft } from './utils'

const log = logger(module)
const { awu } = collections.asynciterable

// TODO: once we have important attributes, use them instead of this hard coded list
const IMPORTANT_ATTRS: Record<string, string[]> = {
  jira: ['name', 'key', 'state', 'enabled', 'projectTypeKey', 'description', 'projects'],
  salesforce: ['name', 'label', 'apiName', 'fullName', 'description', 'helptext'],
  netsuite: ['label', 'name', 'title', 'recordname', 'isinactive', 'recordtype', 'selectrecordtype', 'isprivate', 'preferred', 'ismandatory', 'description'],
}

// TODO: once we have aliases, use aliases
const getReadableName = (elem: Element): string => (
  `${invertNaclCase(elem.elemID.typeName)}.${invertNaclCase(elem.elemID.name)}`
)

const dumpNacl = async (value: Value): Promise<string> => {
  const files = new Map<string, StaticFile>()
  const dummyStaticFileSource = staticFiles.buildInMemStaticFilesSource(files)
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
    // Re-format first line to match element ID formatting
    newData = `${getReadableName(value)}:\n${newData.split('\n').slice(1).join('\n')}`
  } else {
    newData = await parser.dumpValues(value, functions, 0)
  }

  newData = newData.trimStart().trimEnd()

  if (files.size > 0) {
    await awu(files.values()).forEach(async file => {
      newData += `\n\n${file.filepath}:\n${await file.getContent()}`
    })
  }

  return newData
}

const indent = (text: string, level: number, opts: { prefix: string } = { prefix: '' }): string => {
  const indentText = _.repeat('  ', level)
  return text.split('\n').map(line => `${opts.prefix}${indentText}${line}`).join('\n')
}

const removeBracketLines = (dumpedObject: string): string => (
  // We remove the first line that has the opening bracket and the last line to ignore the closing bracket,
  dumpedObject.split('\n').slice(1, -1).join('\n')
)

const dumpImportantValues = async (element: Element, idsToSkip: Set<string>): Promise<string> => {
  const importantValues = {}
  walkOnElement({
    element,
    func: ({ value, path }) => {
      if (
        // We assume important attributes are always at the first nesting level
        (IMPORTANT_ATTRS[path.adapter] ?? []).includes(path.createBaseID().path[0])
        && !idsToSkip.has(path.getFullName())
      ) {
        _.set(importantValues, path.createBaseID().path, value)
        return WALK_NEXT_STEP.SKIP
      }
      return WALK_NEXT_STEP.RECURSE
    },
  })
  return _.isEmpty(importantValues) ? '' : removeBracketLines(await dumpNacl(importantValues)).concat('\n')
}

export type FormatType = 'full' | 'important' | 'key'
export const FormatTypeNames: FormatType[] = ['full', 'important', 'key']


export const isFormatType = (str: string): str is FormatType => (
  FormatTypeNames.includes(str as FormatType)
)

type EncodingLen = (text: string) => number
const dumpValueInFormat = async (
  key: string,
  value: Value,
  format: FormatType,
  maxLength: number,
  encodingLength: EncodingLen,
): Promise<string> => {
  if (format === 'full') {
    let dumpedValue = await dumpNacl(value)
    if (!isElement(value)) {
      dumpedValue = `${key} = ${dumpedValue}`
    }
    if (encodingLength(dumpedValue) < maxLength) {
      return dumpedValue
    }
  }
  if (format === 'important' || format === 'full') {
    if (isElement(value)) {
      const dumpedValue = `${key}:\n${await dumpImportantValues(value, new Set())}`
      if (encodingLength(dumpedValue) < maxLength) {
        return dumpedValue
      }
    }
  }
  return key
}

const getReadableDescriptionOfDetailedChange = async (
  detailedChange: DetailedChange,
  maxValueLength: number,
  preferredFormat: FormatType,
  encodingLength: EncodingLen,
): Promise<string | undefined> => {
  const pathInBaseID = detailedChange.id.createBaseID().path
  const changeKey = pathInBaseID.join('.') || getReadableName(getChangeData(detailedChange))
  if (isAdditionChange(detailedChange)) {
    return `Added ${await dumpValueInFormat(changeKey, detailedChange.data.after, preferredFormat, maxValueLength, encodingLength)}`
  }
  if (isRemovalChange(detailedChange)) {
    return `Removed ${await dumpValueInFormat(changeKey, detailedChange.data.before, preferredFormat, maxValueLength, encodingLength)}`
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


const getReadableModificationDescription = async (
  change: ModificationChange<ChangeDataType>,
  dumpLimit: number,
  preferredFormat: FormatType,
  encodingLength: EncodingLen,
): Promise<string> => {
  const changedElemName = getReadableName(change.data.after)
  const detailedChanges = detailedCompare(
    change.data.before,
    change.data.after,
    { compareListItems: true, createFieldChanges: true },
  )

  // TODO:ORI - in "important" mode, we should summarize the detailed changes more aggressively
  //            currently we emit a line for every detailed change, we should group them at some level

  // Important attributes
  const detailedChangeIDs = new Set(detailedChanges.map(detailedChange => detailedChange.id.getFullName()))
  const dumpedImportant = await dumpImportantValues(change.data.after, detailedChangeIDs)

  const getChangeString = async (valueDumpLimit: number): Promise<string> => {
    const dumpedChanges = await Promise.all(
      detailedChanges.map(detailed => getReadableDescriptionOfDetailedChange(detailed, valueDumpLimit, 'full', encodingLength))
    )
    return `In ${changedElemName}:\n${dumpedImportant}${indent(dumpedChanges.filter(values.isDefined).join('\n'), 1)}`
  }
  const fullChangeString = await getChangeString(dumpLimit)
  if (preferredFormat === 'full' && encodingLength(fullChangeString) < dumpLimit) {
    return fullChangeString
  }
  // Too many changes in a single element, fallback to reporting the changes without the values
  const changesStringWithoutValues = await getChangeString(0)
  if (
    (preferredFormat === 'full' || preferredFormat === 'important')
    && encodingLength(changesStringWithoutValues) < dumpLimit
  ) {
    return changesStringWithoutValues
  }
  // Even reporting the internal IDs without values is not enough, so fallback to just saying "something changed"
  return `Changed ${changedElemName}`
}

type FormatChangesOptions = {
  maxTokens?: number
  modelName?: TiktokenModel
  regexToFilterOut?: string[]
  preferredFormat: FormatType
}
const FORMAT_CHANGES_DEFAULT_OPTIONS = {
  maxTokens: 3500,
  modelName: 'gpt-3.5-turbo',
  preferredFormat: 'full',
} as const
export const formatChangesForPrompt = async (changes: Change[], opts?: FormatChangesOptions): Promise<string[]> => {
  const params = { ...FORMAT_CHANGES_DEFAULT_OPTIONS, ...opts }
  const encoding = encoding_for_model(params.modelName)
  const encodingLength = (text: string): number => encoding.encode(text).length

  const getFormattedChanges = async (changesToFormat: Change[]): Promise<string[]> => {
    const [removals, addOrModify] = _.partition(changesToFormat, isRemovalChange)
    const [additions, modifications] = _.partition(addOrModify, isAdditionChange)

    const formattedAdditions = await Promise.all(
      additions.map(change => getReadableDescriptionOfDetailedChange(
        { ...change, id: change.data.after.elemID },
        params.maxTokens,
        params.preferredFormat,
        encodingLength,
      ))
    )
    const formattedRemovals = await Promise.all(
      removals.map(change => getReadableDescriptionOfDetailedChange(
        { ...change, id: change.data.before.elemID },
        params.maxTokens,
        params.preferredFormat,
        encodingLength,
      ))
    )
    const formattedModifications = await Promise.all(
      modifications.map(change => getReadableModificationDescription(
        change,
        params.maxTokens,
        params.preferredFormat,
        encodingLength,
      ))
    )

    return formattedAdditions
      .concat(formattedRemovals)
      .concat(formattedModifications)
      .filter(values.isDefined)
      .map(changesStr => (params.regexToFilterOut ?? []).reduce((str, regex) => str.replace(new RegExp(regex, 'm'), ''), changesStr))
  }


  // Sort changes by typename first, then type of change
  const formattedChanges = await Promise.all(
    Object.values(_.groupBy(changes, change => getChangeData(change).elemID.typeName))
      .map(getFormattedChanges)
  )

  const changeChunks = chunkBySoft(
    formattedChanges.flat(),
    params.maxTokens,
    formattedChange => encodingLength(formattedChange),
  )

  const formattedChunks = changeChunks.map(chunk => chunk.join('\n'))
  log.debug('Formatted changes token lengths: %s', formattedChunks.map(encodingLength).join('\n'))
  return formattedChunks
}
