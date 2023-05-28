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
import { collections } from '@salto-io/lowerdash'
import { readFileSync } from '@salto-io/file'
import { logger } from '@salto-io/logging'
import { CommandDefAction, createPublicCommandDef } from '../../command_builder'
import { CliExitCode } from '../../types'
import { loadSemanticChangesFromFolder } from './lib/semantic_changes'
import { getCommitMessageForChanges } from './lib/ai_commit_message'
import { ensureDirExists } from './lib/file_operations'

const { awu } = collections.asynciterable
const log = logger(module)

const defaultPromptForCommit = `The user will provide you with a description of the changes made.
  Your output will be only a commit message which describes the effect of these changes.
  The commit message must conform to the semantic commits specification.
  The commit message should include names of the main affected elements.`

const defaultPromptForMerge = `The user will provide you with messages from multiple commits that they would like to squash into a single commit.
Your output will be only a commit message which describes the effect of these changes.
The commit message must conform to the semantic commits specification.
The commit message should include names of the main affected elements.`

const getPrompt = (defaultPrompt: string, filename?: string): string => (
  filename !== undefined ? readFileSync(filename, { encoding: 'utf8' }) : defaultPrompt
)

type CreateCommitMessagesArgs = {
  semanticDiffsFolder: string
  outputMessagesFolder: string
  commitPromptFile?: string
  mergePromptFile?: string
  maxTokens: string
}
const createCommitMessagesAction: CommandDefAction<CreateCommitMessagesArgs> = async ({ input }) => {
  const { semanticDiffsFolder, outputMessagesFolder, commitPromptFile, mergePromptFile, maxTokens } = input
  const numMaxTokens = Number(maxTokens)
  if (Number.isNaN(numMaxTokens)) {
    return CliExitCode.UserInputError
  }

  const outputStepsFolder = `${outputMessagesFolder}/steps`
  ensureDirExists(outputMessagesFolder)
  ensureDirExists(outputStepsFolder)

  const promptForCommit = getPrompt(defaultPromptForCommit, commitPromptFile)
  const promptForMerge = getPrompt(defaultPromptForMerge, mergePromptFile)

  const semanticChanges = loadSemanticChangesFromFolder(semanticDiffsFolder)
  await awu(Object.entries(semanticChanges))
    .map(async ([filename, changes]) => {
      log.debug('Generating message for file %s', filename)
      return {
        filename,
        ...await getCommitMessageForChanges({
          changes,
          promptForCommit,
          promptForMerge,
          maxTokens: numMaxTokens,
        }),
      }
    })
    .forEach(({ filename, message, steps }) => {
      fs.writeFileSync(`${outputMessagesFolder}/${filename}_commit_msg.txt`, message)
      steps.forEach((step, idx) => {
        fs.writeFileSync(`${outputStepsFolder}/${filename}_step_${idx}.txt`, step)
      })
    })
  return CliExitCode.Success
}

export const createCommitMessagesCommand = createPublicCommandDef({
  action: createCommitMessagesAction,
  properties: {
    name: 'create-messages',
    description: 'Create commit messages from semantic diffs',
    positionalOptions: [
      {
        name: 'semanticDiffsFolder',
        required: true,
        type: 'string',
      },
      {
        name: 'outputMessagesFolder',
        required: true,
        type: 'string',
      },
    ],
    keyedOptions: [
      {
        name: 'commitPromptFile',
        alias: 'c',
        type: 'string',
        description: 'path to a file that contains the prompt used for getting a summary of a set of changes',
      },
      {
        name: 'mergePromptFile',
        alias: 'm',
        type: 'string',
        description: 'path to a file that contains the prompt used for merging multiple summaries together',
      },
      {
        name: 'maxTokens',
        alias: 't',
        type: 'string',
        description: 'maximum tokens for response',
        default: '256',
      },
    ],
  },
})
