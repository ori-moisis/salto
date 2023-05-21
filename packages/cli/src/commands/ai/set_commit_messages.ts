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
import { collections } from '@salto-io/lowerdash'
import { logger } from '@salto-io/logging'
import simpleGit from 'simple-git'
import { CommandDefAction, createPublicCommandDef } from '../../command_builder'
import { CliExitCode } from '../../types'
import { loadCommitMessagesFromFolder } from './lib/semantic_changes'
import { resetBranchToCommit } from './lib/git_operations'

const { awu } = collections.asynciterable
const log = logger(module)

type SetCommitMessagesArgs = {
  messagesFolder: string
  repoFolder: string
  branchName: string
  branchBase: string
}

const setCommitMessagesAction: CommandDefAction<SetCommitMessagesArgs> = async ({ input }) => {
  const { messagesFolder, repoFolder, branchName, branchBase } = input
  const git = simpleGit(repoFolder)
  await resetBranchToCommit(git, branchName, branchBase)

  const commitMessages = loadCommitMessagesFromFolder(messagesFolder)
  const hashAndMessage = Object.entries(commitMessages).map(([key, message]) => ({ hash: key.split('_')[1], message }))

  await awu(hashAndMessage).forEach(async ({ hash, message }) => {
    log.debug('Setting message on hash %s', hash)
    await git.raw('cherry-pick', hash)
    await git.raw('commit', '--amend', '-m', message)
  })

  return CliExitCode.Success
}

export const setCommitMessagesCommand = createPublicCommandDef({
  action: setCommitMessagesAction,
  properties: {
    name: 'set-messages',
    description: 'Set commit messages from folder onto a branch',
    positionalOptions: [
      {
        name: 'repoFolder',
        required: true,
        type: 'string',
      },
      {
        name: 'messagesFolder',
        required: true,
        type: 'string',
      },
      {
        name: 'branchBase',
        required: true,
        type: 'string',
      },
      {
        name: 'branchName',
        required: true,
        type: 'string',
      },
    ],
  },
})
