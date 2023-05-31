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
import fs from 'fs'
import simpleGit, { SimpleGit } from 'simple-git'
import { closeAllRemoteMaps, loadLocalWorkspace } from '@salto-io/core'
import { logger } from '@salto-io/logging'
import { Change } from '@salto-io/adapter-api'
import { collections } from '@salto-io/lowerdash'
import { listDir } from './file_operations'
import { resetBranchToCommit } from './git_operations'

const { awu } = collections.asynciterable
const log = logger(module)

type GetSemanticChangesArgs = {
  git: SimpleGit
  repoFolder: string
  fromCommit: string
  toCommit: string
}
export const getSemanticChanges = async (
  { git, repoFolder, fromCommit, toCommit }: GetSemanticChangesArgs
): Promise<Change[]> => {
  const relevantFiles = (await git.raw('diff', '--name-only', fromCommit, toCommit))
    .split('\n')
    .map(name => (
      // Remove envs/<env name>/ from the path if needed
      name.startsWith('envs/')
        ? name.split('/').slice(2).join('/')
        : name
    ))

  log.debug('Relevant files: %s', relevantFiles.join(','))
  await git.checkout(fromCommit)

  // Checkout state file from the target commit already to avoid invalidating everything
  await git.raw('checkout', toCommit, '--', 'salto.config/states')

  // Update workspace cache to ensure we have a baseline
  const wsBefore = await loadLocalWorkspace({ path: repoFolder, changedFiles: relevantFiles })
  await wsBefore.flush()
  await closeAllRemoteMaps()

  // Restore state so the next checkout works cleanly
  await git.raw('checkout', fromCommit, '--', 'salto.config/states')
  await git.checkout(toCommit)

  // Loading now should detect changes
  let changes: Change[] = []
  const changesCallback = (reportedChanges: Change[]): void => {
    changes = reportedChanges
  }
  const wsAfter = await loadLocalWorkspace({ path: repoFolder, changesCallback, changedFiles: relevantFiles })
  await wsAfter.flush() // Must call flush to trigger the actual loading
  await closeAllRemoteMaps()

  log.debug('Got %d changes', changes.length)

  return changes
}


export const loadSemanticChangesFromFolder = (folder: string): Record<string, string[]> => {
  const files = Array.from(listDir(folder)).sort()
  const filesByCommitIdx = _.groupBy(files, filename => filename.split('_part')[0])
  return _.mapValues(filesByCommitIdx, filenames => filenames.map(name => fs.readFileSync(`${folder}/${name}`, { encoding: 'utf8' })))
}


// Should not be here
export const loadCommitMessagesFromFolder = (folder: string): Record<string, string> => {
  const files = Array.from(listDir(folder)).sort()
  return Object.fromEntries(
    files.map(filename => ([filename, fs.readFileSync(`${folder}/${filename}`, { encoding: 'utf8' })]))
  )
}

type AmendCommitMessagesArgs = {
  repoDir: string
  branchName: string
  branchBase: string
  commitMessages: { hash: string; message: string; idx: number }[]
}

export const amendCommitMessages = async (
  { repoDir, branchName, branchBase, commitMessages }: AmendCommitMessagesArgs
): Promise<void> => {
  const git = simpleGit(repoDir)
  await resetBranchToCommit(git, branchName, branchBase)
  await awu(commitMessages).forEach(async ({ hash, message }) => {
    await git.raw('cherry-pick', hash)
    await git.raw('commit', '--amend', '-m', message)
  })
}
