/*
*                      Copyright 2024 Salto Labs Ltd.
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
import { EOL, tmpdir } from 'os'
import * as fs from 'fs'
import express, { Response } from 'express'
import { v4 as uuid } from 'uuid'
import { cleanWorkspace } from '@salto-io/core'
import { ElementSelector, nacl, parser, ProviderOptionsS3, StateConfig, staticFiles, WorkspaceComponents } from '@salto-io/workspace'
import { resolvePath, walkOnElement, WALK_NEXT_STEP } from '@salto-io/adapter-utils'
import { ElemID, InstanceElement, isContainerType, isInstanceElement, isObjectType, isPrimitiveType, isPrimitiveValue, isReferenceExpression, isTemplateExpression, PrimitiveTypes, ReadOnlyElementsSource, StaticFile, TypeElement } from '@salto-io/adapter-api'
import { collections } from '@salto-io/lowerdash'
import { logger } from '@salto-io/logging'
import { createObjectCsvWriter } from 'csv-writer'
import { getUserBooleanInput } from '../callbacks'
import { header, formatCleanWorkspace, formatCancelCommand, formatStepStart, formatStepFailed, formatStepCompleted } from '../formatter'
import { outputLine, errorOutputLine } from '../outputer'
import Prompts from '../prompts'
import { CliExitCode } from '../types'
import { createCommandGroupDef, createWorkspaceCommand, WorkspaceCommandAction } from '../command_builder'
import { EnvArg, ENVIRONMENT_OPTION } from './common/env'

const { awu } = collections.asynciterable
const { makeArray } = collections.array

const log = logger(module)

type CleanArgs = {
  force: boolean
} & WorkspaceComponents

export const cleanAction: WorkspaceCommandAction<CleanArgs> = async ({
  input: { force, ...cleanArgs },
  output,
  workspace,
}): Promise<CliExitCode> => {
  const shouldCleanAnything = Object.values(cleanArgs).some(shouldClean => shouldClean)
  if (!shouldCleanAnything) {
    outputLine(header(Prompts.EMPTY_PLAN), output)
    outputLine(EOL, output)
    return CliExitCode.UserInputError
  }
  if (cleanArgs.staticResources && !(cleanArgs.state && cleanArgs.cache && cleanArgs.nacl)) {
    errorOutputLine('Cannot clear static resources without clearing the state, cache and nacls', output)
    outputLine(EOL, output)
    return CliExitCode.UserInputError
  }

  outputLine(header(
    formatCleanWorkspace(cleanArgs)
  ), output)
  if (!(force || await getUserBooleanInput(Prompts.SHOULD_EXECUTE_PLAN))) {
    outputLine(formatCancelCommand, output)
    return CliExitCode.Success
  }

  outputLine(formatStepStart(Prompts.CLEAN_STARTED), output)

  try {
    await cleanWorkspace(workspace, cleanArgs)
  } catch (e) {
    errorOutputLine(formatStepFailed(Prompts.CLEAN_FAILED(e.toString())), output)
    return CliExitCode.AppError
  }

  outputLine(formatStepCompleted(Prompts.CLEAN_FINISHED), output)
  outputLine(EOL, output)
  return CliExitCode.Success
}

const wsCleanDef = createWorkspaceCommand({
  properties: {
    name: 'clean',
    description: 'Maintenance command for cleaning workspace data. This operation cannot be undone, it\'s highly recommended to backup the workspace data before executing it.',
    keyedOptions: [
      {
        name: 'force',
        alias: 'f',
        description: 'Do not ask for approval before applying the changes',
        type: 'boolean',
      },
      {
        name: 'nacl',
        alias: 'n',
        description: 'Do not remove the nacl files',
        type: 'boolean',
        default: true,
      },
      {
        name: 'state',
        alias: 's',
        description: 'Do not clear the state',
        type: 'boolean',
        default: true,
      },
      {
        name: 'cache',
        alias: 'c',
        description: 'Do not clear the cache',
        type: 'boolean',
        default: true,
      },
      {
        name: 'staticResources',
        alias: 'r',
        description: 'Do not remove remove the static resources',
        type: 'boolean',
        default: true,
      },
      {
        name: 'credentials',
        alias: 'l',
        description: 'Clear the account login credentials',
        type: 'boolean',
        default: false,
      },
      {
        name: 'accountConfig',
        alias: 'g',
        description: 'Restore account configuration to default',
        type: 'boolean',
        default: false,
      },
    ],
  },
  action: cleanAction,
})

type CacheUpdateArgs = {}
export const cacheUpdateAction: WorkspaceCommandAction<CacheUpdateArgs> = async ({
  workspace,
  output,
}) => {
  outputLine('Updating workspace cache', output)
  await workspace.flush()
  return CliExitCode.Success
}

const cacheUpdateDef = createWorkspaceCommand({
  properties: {
    name: 'update',
    description: 'Update the workspace cache',
  },
  action: cacheUpdateAction,
})

const cacheGroupDef = createCommandGroupDef({
  properties: {
    name: 'cache',
    description: 'Commands for workspace cache administration',
  },
  subCommands: [
    cacheUpdateDef,
  ],
})

type SetStateProviderArgs = {
  provider?: StateConfig['provider']
} & Partial<ProviderOptionsS3>

export const setStateProviderAction: WorkspaceCommandAction<SetStateProviderArgs> = async ({
  workspace,
  input,
  output,
}) => {
  const { provider, bucket, prefix } = input
  outputLine(`Setting state provider ${provider} for workspace`, output)
  const stateConfig: StateConfig = { provider: provider ?? 'file' }

  if (provider === 's3') {
    if (bucket === undefined) {
      errorOutputLine('Must set bucket name with provider of type s3', output)
      return CliExitCode.UserInputError
    }
    stateConfig.options = { s3: { bucket, prefix } }
  }
  if (provider !== 's3' && bucket !== undefined) {
    errorOutputLine('bucket argument is only valid with provider type s3', output)
    return CliExitCode.UserInputError
  }

  await workspace.updateStateProvider(provider === undefined ? undefined : stateConfig)
  return CliExitCode.Success
}

const setStateProviderDef = createWorkspaceCommand({
  action: setStateProviderAction,
  properties: {
    name: 'set-state-provider',
    description: 'Set the location where state data will be stored',
    keyedOptions: [
      {
        name: 'provider',
        alias: 'p',
        type: 'string',
        choices: ['file', 's3'],
        required: false,
      },
      {
        name: 'bucket',
        type: 'string',
        description: 'When provider is S3, the bucket name were state data can be stored',
      },
      {
        name: 'prefix',
        type: 'string',
        description: 'A prefix inside the bucket where files will be stored',
      },
    ],
  },
})

const getValueExamples = async (
  instances: AsyncIterable<InstanceElement>,
  fields: string[]
): Promise<Record<string, string[]>> => {
  // Stop iteration if we either
  // - saw enough unique values
  // - iterated over many values and saw enough different options
  const addAndCheckIfCanStop = (set: Set<unknown>, val: unknown, isHighIteration: boolean): boolean => {
    if (set.size > 5) {
      return true
    }
    if (isPrimitiveValue(val) && val != null) {
      set.add(val)
    }
    if (isReferenceExpression(val)) {
      set.add(val.elemID.getFullName())
    }
    if (isHighIteration && set.size > 1) {
      return true
    }
    if (isHighIteration && set.size <= 1 && val == null) {
      // Everything we checked so far was null, time to give up
      return true
    }
    return false
  }

  const fieldValues = awu(instances)
    .map(inst => Object.fromEntries(fields.map(field => [field, _.get(inst.value, field)])))
  const valueSets = Object.fromEntries(fields.map(field => [field, new Set<string>()]))
  let iteration = 0
  for await (const val of fieldValues) {
    iteration += 1
    const isHighIteration = iteration > 100
    const canStop = fields.map(field => addAndCheckIfCanStop(valueSets[field], val[field], isHighIteration))

    if (canStop.every(stop => stop)) {
      break
    }
  }
  return _.pickBy(_.mapValues(valueSets, set => Array.from(set)), val => !_.isEmpty(val))
}

type TypeFieldDescriptions = {
  fieldTypes: Record<string, Record<string, string>>
  examples: Record<string, string[]>
}
const getTypeFieldDescriptions = async (
  rootType: TypeElement,
  elementSource: ReadOnlyElementsSource,
  maxDepth = 5,
): Promise<TypeFieldDescriptions> => {
  const fieldTypes: Record<string, Record<string, string>> = {}
  const primitiveTypeNames: Record<PrimitiveTypes, string> = {
    [PrimitiveTypes.BOOLEAN]: 'bool',
    [PrimitiveTypes.NUMBER]: 'number',
    [PrimitiveTypes.STRING]: 'string',
    [PrimitiveTypes.UNKNOWN]: 'unknown',
  }
  const primitiveFields: string[] = []
  const addFields = async (type: TypeElement, path: string[] = []): Promise<void> => {
    if (path.length >= maxDepth || Object.prototype.hasOwnProperty.call(fieldTypes, type.elemID.typeName)) {
      return
    }
    if (isObjectType(type)) {
      const fieldMap: Record<string, string> = {}
      fieldTypes[type.elemID.typeName] = fieldMap
      await awu(Object.values(type.fields))
        .forEach(async field => {
          const fieldType = await field.getType(elementSource)
          if (isPrimitiveType(fieldType)) {
            fieldMap[field.name] = primitiveTypeNames[fieldType.primitive]
            primitiveFields.push(path.concat(field.name).join('.'))
          } else {
            fieldMap[field.name] = field.refType.elemID.typeName
            await addFields(fieldType, path.concat(field.name))
          }
        })
    } else if (isContainerType(type)) {
      await addFields(await type.getInnerType(elementSource), path.concat('0'))
    }
  }

  await addFields(rootType)

  const instances = awu(await elementSource.list())
    .filter(id => id.typeName === rootType.elemID.typeName && id.idType === 'instance')
    .map(id => elementSource.get(id))
    .filter(isInstanceElement)
  log.debug('Getting examples for type %s fields %o', rootType.elemID.typeName, primitiveFields)
  const examples = _.mapKeys(
    await getValueExamples(instances, primitiveFields),
    (_val, key) => key.replace('.0.', '.*.')
  )
  return { fieldTypes, examples }
}

type CreateRegExpOptions = {
  caseSensitive?: boolean
  exact?: boolean
  forcePattern?: boolean
}
const createRegex = (selector: string, opts?: CreateRegExpOptions): RegExp => {
  let actualSelector = selector === '*' ? '.*' : selector
  const forcePattern = !opts?.exact || (opts?.forcePattern && !selector.includes('*'))
  if (!actualSelector.startsWith('^')) {
    actualSelector = `^${forcePattern ? '.*' : ''}${actualSelector}`
  }
  if (!actualSelector.endsWith('$')) {
    actualSelector = `${actualSelector}${forcePattern ? '.*' : ''}$`
  }
  return new RegExp(actualSelector, opts?.caseSensitive ? undefined : 'i')
}

type SelectorInput = {
  idType: string
  account?: string
  typeName?: string
  id?: string
  caseSensitive?: boolean
  exact?: boolean
  referencedBy?: SelectorInput
  value: Array<{ field: string; value: string }>
}
const createSelector = (input: SelectorInput): ElementSelector => ({
  adapterSelector: createRegex(input.account ?? '.*', input),
  idTypeSelector: 'instance',
  typeNameSelector: createRegex(input.typeName ?? '.*', input),
  nameSelectors: [createRegex(input.id ?? '.*', input)],
  caseInsensitive: !input.caseSensitive,
  referencedBy: input.referencedBy === undefined
    ? undefined
    : createSelector(input.referencedBy),
  value: input.value === undefined
    ? undefined
    : makeArray(input.value).map(({ field, value }) => ({ field, value: createRegex(value, input) })),
  origin: `${input.account?.replace('.', '') ?? '*'}.${input.typeName?.replace('.', '') ?? '*'}.instance.${input.id?.replace('.', '') ?? '*'}`,
})

const isFieldNameMatch = (fieldPattern: string, fieldPath: readonly string[]): boolean => {
  // People are not great at giving nested field paths, especially when lists are involved
  // so to help it a little, we will match fields even if query "missed" a few layers
  // that means we match if all the parts of the pattern appear in the field path, in the correct order
  // but we do allow the field path to have additional parts, e.g
  // the pattern "projects.projectId" will match "projects.0.projectId"
  const patternParts = fieldPattern.split('.')
  if (patternParts.length > fieldPath.length) {
    return false
  }
  let lastIdx = 0
  const patternPartsIndices = patternParts.map(part => {
    if (lastIdx === -1) {
      return -1
    }
    const partIdx = fieldPath.slice(lastIdx).findIndex((fieldPart => fieldPart === part))
    if (partIdx === -1) {
      lastIdx = -1
      return partIdx
    }
    lastIdx += partIdx
    return lastIdx + partIdx
  })
  if (patternPartsIndices.includes(-1)) {
    return false
  }
  if (_.isEqual(patternPartsIndices, patternPartsIndices.sort())) {
    return true
  }
  return false
}

type SearchResult = {
  id: string
  value: Record<string, unknown>
}

const searchForContent = async (
  inst: InstanceElement,
  content?: RegExp,
  fieldsToReturn?: string[],
): Promise<SearchResult> => {
  // TODO: this does not search in static files content
  const result: Record<string, unknown> = {}

  const getValueToMatch = (value: unknown): string | undefined => {
    if (isPrimitiveValue(value)) {
      return _.toString(value)
    }
    if (isReferenceExpression(value)) {
      return value.elemID.getFullName()
    }
    if (isTemplateExpression(value)) {
      return parser.dumpValueSync(value)
    }
    return undefined
  }
  let anyContentMatch = false
  walkOnElement({
    element: inst,
    func: ({ value, path }) => {
      const fieldPath = path.createTopLevelParentID().path
      if (content !== undefined) {
        const valueToMatch = getValueToMatch(value)
        if (valueToMatch !== undefined && content.test(valueToMatch)) {
          anyContentMatch = true
          _.set(result, fieldPath.join('.'), valueToMatch)
          return WALK_NEXT_STEP.SKIP
        }
      }
      if (fieldsToReturn?.some(fieldToReturn => isFieldNameMatch(fieldToReturn, fieldPath))) {
        _.set(result, fieldPath.join('.'), getValueToMatch(value) ?? parser.dumpValueSync(value))
        return WALK_NEXT_STEP.SKIP
      }
      return WALK_NEXT_STEP.RECURSE
    },
  })

  if (content !== undefined && !anyContentMatch) {
    // This instance is not a match based on content, so we should not return a value for it
    return { id: inst.elemID.getFullName(), value: {} }
  }

  return { id: inst.elemID.getFullName(), value: result }
}

type ServeAPIArgs = {
  port: string
} & EnvArg

export const serveAPIAction: WorkspaceCommandAction<ServeAPIArgs> = async ({
  workspace,
  input,
  output,
}) => {
  const { port } = input
  outputLine(`Starting server for workspace ${workspace.name} (${workspace.uid})`, output)

  const app = express()
  app.use(express.json())
  // eslint-disable-next-line consistent-return
  app.use((req, res, next) => {
    const token = req.headers['ngrok-skip-browser-warning']
    if (!token) {
      return res.status(401).send('Access denied. No token provided.')
    }
    if (token === 'SaltoAPIServerToken') {
      next()
    } else {
      res.status(403).send('Invalid token')
    }
  })
  app.use((req, _res, next) => {
    if (_.isEmpty(req.body)) {
      log.debug('Handling request %s', req.url)
    } else {
      log.debug('Handling request %s with body:\n%o', req.url, req.body)
    }
    next()
  })

  const orgs = ['First Org'].map(name => ({ name, id: uuid() }))
  const envNames = ['UAT', 'Staging', 'Production']
  const envsPerOrg = new Map(orgs.map(({ id }) => [id, envNames.map(name => ({ id: uuid(), name }))]))

  // Preload elements
  const elements = await workspace.elements()

  const sendResponse = (res: Response, value: unknown): void => {
    log.debug('Sending response %o', value)
    res.status(200).contentType('application/json').send(value)
  }

  app.get('/user', (_req, res) => {
    sendResponse(res, { name: 'Ori Moisis', orgs })
  })
  app.get('/org/:orgId', (req, res) => {
    const { orgId } = req.params
    const environments = envsPerOrg.get(orgId)
    if (environments == null) {
      res.sendStatus(404)
      return
    }
    sendResponse(res, { environments })
  })
  app.get('/org/:orgId/env/:envId/type', async (_req, res) => {
    const typeNames = await (awu(await elements.list())
      .filter(id => id.idType === 'instance')
      .map(id => id.typeName)
      .uniquify(name => name))
      .toArray()
    sendResponse(res, { typeNames })
  })
  app.get('/org/:orgId/env/:envId/type/:typeName', async (req, res) => {
    const typeName = req.params.typeName as string
    const typeId = await awu(await elements.list()).find(id => id.idType === 'type' && id.typeName === typeName)
    if (typeId == null) {
      res.sendStatus(404)
      return
    }
    const type = await elements.get(typeId)
    if (type == null) {
      res.sendStatus(404)
      return
    }
    const fieldDescriptions = await getTypeFieldDescriptions(type, elements)
    sendResponse(res, fieldDescriptions)
  })
  app.get('/org/:orgId/env/:envId/type/:typeName/index', async (req, res) => {
    const { typeName } = req.params
    const { columns } = req.query
    const typeId = await awu(await elements.list()).find(id => id.idType === 'type' && id.typeName === typeName)
    if (typeId == null) {
      res.sendStatus(404)
      return
    }
    const type = await elements.get(typeId) as TypeElement
    if (!isObjectType(type)) {
      res.sendStatus(404)
      return
    }

    const tmpDir = await fs.promises.mkdtemp(`${await fs.promises.realpath(tmpdir())}/`)
    const csvPath = `${tmpDir}/x.csv`
    const cols = makeArray(columns) as string[] ?? await awu(Object.values(type.fields))
      .filter(async field => isPrimitiveType(await field.getType(elements)))
      .map(field => field.name)
      .toArray()
    const writer = createObjectCsvWriter({ path: csvPath, header: ['name'].concat(cols).map(col => ({ id: col, title: col })) })
    await writer.writeRecords(
      await awu(await elements.list())
        .filter(id => id.idType === 'instance' && id.typeName === typeName)
        .map(id => elements.get(id))
        .filter(isInstanceElement)
        .map(inst => ({
          name: inst.elemID.name,
          ...Object.fromEntries(
            cols
              .map(key => [key, resolvePath(inst, inst.elemID.createNestedID(...key.split('.')))])
              .filter(([_key, value]) => isPrimitiveValue(value))
          ),
        }))
        .toArray()
    )
    res.status(200).contentType('csv')
    res.sendFile(csvPath)
    await fs.promises.rmdir(tmpDir, { recursive: true })
  })
  app.post('/org/:orgId/env/:envId/search', async (req, res) => {
    try {
      type ReqBody = {
        selectors: Array<SelectorInput>
        searchContent: string
        fieldsToReturn: Array<string>
      }
      const { selectors, searchContent, fieldsToReturn } = req.body as ReqBody
      if (selectors == null && searchContent == null) {
        log.debug('failed to search, no criteria provided')
        res.status(400).send('at least one of selectors or searchContent must be provided')
        return
      }
      const convertedSelectors = makeArray(selectors).map(createSelector)
      const ids = await awu(
        convertedSelectors.length === 0
          ? await elements.list()
          : awu(await workspace.getElementIdsBySelectors(convertedSelectors, { source: 'all' }))
      ).toArray()

      if (searchContent === undefined && fieldsToReturn === undefined) {
        sendResponse(res, { results: ids.map(id => ({ id: id.getFullName() })) })
        return
      }
      const contentRegex = searchContent === undefined
        ? undefined
        : createRegex(searchContent, { forcePattern: true })
      const results = await awu(ids)
        .map(id => elements.get(id))
        .filter(isInstanceElement)
        .map(inst => searchForContent(inst, contentRegex, fieldsToReturn))
        // If this is a content search, only return non-empty results
        .filter(({ value }) => (contentRegex === undefined || !_.isEmpty(value)))
        .toArray()
      sendResponse(res, { results })
    } catch (e) {
      log.debug('failed to search with error: %o', e)
      res.status(400).send(e.toString())
    }
  })
  app.post('/org/:orgId/env/:envId/element', async (req, res) => {
    type ReqBody = {
      id: string
    }
    const { id } = req.body as ReqBody
    const elem = await elements.get(ElemID.fromFullName(id))
    const files = new Map<string, StaticFile>()
    const staticFilesSource = staticFiles.buildInMemStaticFilesSource(files)
    const functions = nacl.getFunctions(staticFilesSource)
    const value = await parser.dumpElements([elem], functions)
    sendResponse(res, { value, files: Array.from(files.keys()) })
  })
  app.post('/org/:orgId/env/:envId/element/files', async (req, res) => {
    type ReqBody = {
      id: string
      files: Array<string>
    }
    const { id, files } = req.body as ReqBody
    const elem = await elements.get(ElemID.fromFullName(id))
    const elemFiles = nacl.getNestedStaticFiles(elem)
    const fileContent = await awu(elemFiles)
      .filter(file => files.includes(file.filepath))
      .map(async file => ({ name: file.filepath, content: await file.getContent() }))
      .toArray()
    sendResponse(res, { fileContent })
  })

  const server = app.listen(Number(port))
  await getUserBooleanInput('Enter something to shutdown')
  server.close()
  return CliExitCode.Success
}

const serverAPIDef = createWorkspaceCommand({
  action: serveAPIAction,
  properties: {
    name: 'serve-api',
    description: 'Start API server',
    keyedOptions: [
      {
        name: 'port',
        alias: 'p',
        type: 'string',
        default: '8081',
      },
      ENVIRONMENT_OPTION,
    ],
  },
})

// Group definition
const wsGroupDef = createCommandGroupDef({
  properties: {
    name: 'workspace',
    description: 'Workspace administration commands',
  },
  subCommands: [
    wsCleanDef,
    cacheGroupDef,
    setStateProviderDef,
    serverAPIDef,
  ],
})

export default wsGroupDef
