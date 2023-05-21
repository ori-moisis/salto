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
import fs from 'fs'
import simpleGit from 'simple-git'
import { collections } from '@salto-io/lowerdash'
import { logger } from '@salto-io/logging'
import { CommandDefAction, createPublicCommandDef } from '../../command_builder'
import { CliExitCode } from '../../types'
import { resetBranchToCommit } from './lib/git_operations'
import { outputLine } from '../../outputer'

const { awu } = collections.asynciterable
const log = logger(module)

const operationEndBranch = (operation: string): string => `operation_${operation}`
const operationStartBranch = (operation: string): string => `operation_${operation}_start`

type CommitInterval = 'fetch' | 'daily' | 'weekly'
type CreateBranchArgs = {
  repoFolder: string
  operationsFilename: string
  outputBranchName: string
  remote: string
  commitInterval: CommitInterval
}
const commitIntervalInDays: Record<CommitInterval, number> = {
  fetch: 0,
  daily: 1,
  weekly: 7,
}

const getNextCommitDate = (lastCommitDate: Date, currCommitDate: Date, interval: CommitInterval): Date => {
  if (interval === 'fetch') {
    return currCommitDate
  }
  const next = new Date(lastCommitDate)
  next.setDate(next.getDate() + commitIntervalInDays[interval])
  return next
}

const createBranchAction: CommandDefAction<CreateBranchArgs> = async ({ input, output }) => {
  const { operationsFilename, outputBranchName, repoFolder, remote, commitInterval } = input

  const git = simpleGit(repoFolder)

  const commitIfNeeded = async (messages: string[], date: Date): Promise<void> => {
    if (messages.length > 0) {
      if ((await git.status()).files.every(file => file.path.startsWith('salto.config/states'))) {
        log.debug('skipping commit of only state file(s) with message %s', messages.join('\n'))
      } else {
        log.debug('commit changes until date %s', date.toISOString())
        await git.raw('-c', 'user.name=salto', '-c', 'user.email=ai@salto.io', 'commit', '--date', date.toISOString(), '-m', messages.join('\n'))
      }
    }
  }

  const getCommitDate = async (commit: string): Promise<Date> => (
    new Date(Date.parse((await git.log([commit, '-n', '1'])).latest?.date ?? ''))
  )

  const operationNames = fs.readFileSync(operationsFilename, { encoding: 'utf8' }).split('\n').filter(line => line)

  const branchAtStart = (await git.branch()).current

  // if we are fetching from remote, ensure all branches exist
  if (remote) {
    const branchNames = operationNames.flatMap(op => [operationStartBranch(op), operationEndBranch(op)])
    log.debug('fetching branches for operations %s', operationNames.join(','))
    const res = await git.raw('fetch', remote, ...branchNames.map(name => `${name}:${name}`))
    log.debug('fetch result: %s', res)
  }


  // Create target branch on first operation start point
  await resetBranchToCommit(git, outputBranchName, operationStartBranch(operationNames[0]))

  let nextCommitDate = getNextCommitDate(
    await getCommitDate(outputBranchName),
    await getCommitDate(outputBranchName),
    commitInterval,
  )
  let messages: string[] = []
  await awu(operationNames).forEach(async operation => {
    outputLine(`handling operation ${operation}`, output)
    const opEndDate = await getCommitDate(operationEndBranch(operation))

    if (opEndDate > nextCommitDate) {
      await commitIfNeeded(messages, nextCommitDate)
      messages = []
      nextCommitDate = getNextCommitDate(nextCommitDate, opEndDate, commitInterval)
    }

    log.debug('checkout files from %s', operationEndBranch(operation))
    await git.raw('checkout', operationEndBranch(operation), '--', '.')

    messages.push(`Operation ${operation} at ${opEndDate.toISOString()}`)
  })
  await commitIfNeeded(messages, nextCommitDate)

  await git.checkout(branchAtStart)

  return CliExitCode.Success
}

export const createBranchCommand = createPublicCommandDef({
  action: createBranchAction,
  properties: {
    name: 'create-branch',
    description: 'Create a branch from a list of operation uuids',
    positionalOptions: [
      {
        name: 'repoFolder',
        required: true,
        type: 'string',
      },
      {
        name: 'operationsFilename',
        required: true,
        type: 'string',
      },
      {
        name: 'outputBranchName',
        required: true,
        type: 'string',
      },
    ],
    keyedOptions: [
      {
        name: 'remote',
        alias: 'r',
        type: 'string',
        description: 'if using branches from a remote, put the remote name here, e.g origin',
        default: '',
      },
      {
        name: 'commitInterval',
        alias: 'i',
        type: 'string',
        choices: ['fetch', 'daily', 'weekly'],
        default: 'fetch',
      },
    ],
  },
})
