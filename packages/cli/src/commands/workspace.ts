/*
 * Copyright 2024 Salto Labs Ltd.
 * Licensed under the Salto Terms of Use (the "License");
 * You may not use this file except in compliance with the License.  You may obtain a copy of the License at https://www.salto.io/terms-of-use
 *
 * CERTAIN THIRD PARTY SOFTWARE MAY BE CONTAINED IN PORTIONS OF THE SOFTWARE. See NOTICE FILE AT https://github.com/salto-io/salto/blob/main/NOTICES
 */
import { EOL } from 'os'
import path from 'path'
import _ from 'lodash'
import { createWriteStream, readFile } from '@salto-io/file'
import { cleanWorkspace, closeAllRemoteMaps, createRemoteMapCreator, loadLocalWorkspace } from '@salto-io/core'
import { collections, values } from '@salto-io/lowerdash'
import { safeJsonStringify } from '@salto-io/adapter-utils'
import { nacl, ProviderOptionsS3, serialization, StateConfig, WorkspaceComponents } from '@salto-io/workspace'
import { isElement } from '@salto-io/adapter-api'
import { getUserBooleanInput } from '../callbacks'
import {
  header,
  formatCleanWorkspace,
  formatCancelCommand,
  formatStepStart,
  formatStepFailed,
  formatStepCompleted,
} from '../formatter'
import { outputLine, errorOutputLine } from '../outputer'
import Prompts from '../prompts'
import { CliExitCode } from '../types'
import {
  CommandDefAction,
  createCommandGroupDef,
  createPublicCommandDef,
  createWorkspaceCommand,
  WorkspaceCommandAction,
} from '../command_builder'

const { awu } = collections.asynciterable

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

  outputLine(header(formatCleanWorkspace(cleanArgs)), output)
  if (!(force || (await getUserBooleanInput(Prompts.SHOULD_EXECUTE_PLAN)))) {
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
    description:
      "Maintenance command for cleaning workspace data. This operation cannot be undone, it's highly recommended to backup the workspace data before executing it.",
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
export const cacheUpdateAction: WorkspaceCommandAction<CacheUpdateArgs> = async ({ workspace, output }) => {
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

type CacheVerifyArgs = {
  pathsFile: string
  outputPath: string
  cacheDir: string
  FindAll: boolean
}
const cacheVerifyAction: CommandDefAction<CacheVerifyArgs> = async ({ input, output }) => {
  // In theory we should run await workspace.flush() before we start to make sure the cache is up to date
  // but we want this to run fast, so we don't want to actually load the workspace

  const badPathsFile = createWriteStream(input.outputPath)
  const paths = (await readFile(input.pathsFile)).toString().split('\n')

  await awu(paths).forEach(async (workspacePath, idx) => {
    const pathName = path.basename(path.dirname(workspacePath))
    const println = (msg: string): void => {
      output.stdout.write(`${pathName}: ${msg}\n`)
      output.stdout.write(`Folder ${idx} of ${paths.length}\r`)
    }

    let envName: string
    try {
      const workspace = await loadLocalWorkspace({
        path: workspacePath,
        ignoreFileChanges: true,
        persistent: false,
      })
      envName = workspace.currentEnv()
    } catch (e) {
      println(`Invalid workspace: ${e}`)
      return
    }
    if (!envName) {
      return
    }

    const remoteMapCreator = createRemoteMapCreator(path.resolve(path.join(workspacePath, input.cacheDir)))
    const elementsWithStaticFilesMap = await remoteMapCreator<string[]>({
      namespace: `workspace-${envName}-referencedStaticFiles`,
      serialize: async val => safeJsonStringify(val),
      deserialize: data => JSON.parse(data),
      persistent: false,
    })
    const elementsWithStaticFiles = await awu(elementsWithStaticFilesMap.keys()).toArray()
    if (elementsWithStaticFiles.length > 500) {
      println(`Checking ${elementsWithStaticFiles.length} elements`)
    }

    const remoteMapsToLoad = [
      `naclFileSource-envs/${envName}-merged`,
      'naclFileSource--merged', // Common nacl source, just in case
      `multi_env-${envName}-merged`,
      `workspace-${envName}-merged`,
      `state-${envName}-elements`,
    ]
    const remoteMaps = await Promise.all(
      remoteMapsToLoad.map(namespace =>
        remoteMapCreator({
          namespace,
          serialize: element => serialization.serialize([element], 'keepRef'),
          deserialize: s => serialization.deserializeSingleElement(s, async staticFile => staticFile),
          persistent: false,
        }),
      ),
    )

    const issueIterator = awu(elementsWithStaticFiles)
      .map(async id => {
        const elements = await Promise.all(remoteMaps.map(remoteMap => remoteMap.get(id)))
        const definedElements = elements.filter(values.isDefined).filter(isElement)
        if (definedElements.length !== 4) {
          // This can happen if there is no state cache
          println(`Not enough elements found for id ${id}, found ${definedElements.length} elements`)
        }
        const staticFilesByName = _.groupBy(definedElements.flatMap(nacl.getNestedStaticFiles), file => file.filepath)
        const mismatchedFiles = Object.entries(staticFilesByName)
          .map(([filepath, files]) => {
            const hashes = files.map(f => f.hash)
            if (new Set(hashes).size !== 1) {
              return { filepath, hashes }
            }
            return undefined
          })
          .filter(values.isDefined)
        if (mismatchedFiles.length > 0) {
          return { id, mismatchedFiles }
        }
        return undefined
      })
      .filter(values.isDefined)

    const issues = input.FindAll ? await issueIterator.toArray() : [await issueIterator.peek()].filter(values.isDefined)

    await Promise.all(remoteMaps.map(remoteMap => remoteMap.close()))
    await closeAllRemoteMaps()

    if (issues.length > 0) {
      println('Found issues in cache')
      badPathsFile.write(`${workspacePath}\n`)
      issues.forEach(({ id, mismatchedFiles }) => {
        println(`${id}: ${mismatchedFiles.map(({ filepath }) => filepath)}`)
      })
      return
    }
    println('Cache is valid')
  })
  badPathsFile.close()
  return CliExitCode.Success
}

const cacheVerifyDef = createPublicCommandDef({
  properties: {
    name: 'verify',
    description: 'Verify cache static files are consistent',
    keyedOptions: [
      {
        name: 'cacheDir',
        type: 'string',
        description: 'Relative location of the cache dir (relative to workspace path)',
      },
      {
        name: 'FindAll',
        type: 'boolean',
        description: 'Find all files with non matching hashes (default - stop on the first mismatch)',
      },
      {
        name: 'pathsFile',
        type: 'string',
        description: 'file with all the paths to check',
      },
      {
        name: 'outputPath',
        type: 'string',
        description: 'file name to write all invalid paths to',
        default: 'bad.txt',
      },
    ],
  },
  action: cacheVerifyAction,
})

const cacheGroupDef = createCommandGroupDef({
  properties: {
    name: 'cache',
    description: 'Commands for workspace cache administration',
  },
  subCommands: [cacheUpdateDef, cacheVerifyDef],
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

// Group definition
const wsGroupDef = createCommandGroupDef({
  properties: {
    name: 'workspace',
    description: 'Workspace administration commands',
  },
  subCommands: [wsCleanDef, cacheGroupDef, setStateProviderDef],
})

export default wsGroupDef
