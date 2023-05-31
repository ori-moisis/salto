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
import { getChangeData } from '@salto-io/adapter-api'
import { CommandDefAction, createPublicCommandDef } from '../../command_builder'
import { CliExitCode } from '../../types'
import { getSemanticChanges } from './lib/semantic_changes'
import { formatChangesForPrompt, FormatType, FormatTypeNames } from './lib/format_change'
import { ensureDirExists } from './lib/file_operations'
import { outputLine } from '../../outputer'

const { awu } = collections.asynciterable

type CreateSemanticDiffsArgs = {
  branchName: string
  baseCommit: string
  outputFolder: string
  repoFolder: string
  maxTokens: string
  typesToIgnore?: string[]
  regexToFilterOut?: string[]
  preferredFormat: FormatType
}

const createSemanticDiffsAction: CommandDefAction<CreateSemanticDiffsArgs> = async ({ input, output }) => {
  const {
    branchName,
    baseCommit,
    outputFolder,
    repoFolder,
    maxTokens,
    regexToFilterOut,
    typesToIgnore,
    preferredFormat,
  } = input

  const numMaxTokens = Number(maxTokens)
  if (Number.isNaN(numMaxTokens)) {
    return CliExitCode.UserInputError
  }

  ensureDirExists(outputFolder)

  const git = simpleGit(repoFolder)
  const commits = await git.log({ from: baseCommit, to: branchName })

  let prevCommit = baseCommit

  const reversedCommits = Array.from(commits.all).reverse()
  await awu(reversedCommits).forEach(async (commit, idx) => {
    outputLine(`handling commit ${commit.hash}`, output)
    const changes = await getSemanticChanges({ git, repoFolder, fromCommit: prevCommit, toCommit: commit.hash })
    const filteredChanges = changes.filter(
      change => !(typesToIgnore ?? []).includes(getChangeData(change).elemID.typeName)
    )
    const formattedChanges = await formatChangesForPrompt(
      filteredChanges,
      { maxTokens: numMaxTokens, regexToFilterOut, preferredFormat },
    )
    formattedChanges.forEach((part, partIdx) => {
      fs.writeFileSync(`${outputFolder}/${idx.toString().padStart(3, '0')}_${commit.hash}_part${partIdx}.txt`, part)
    })
    prevCommit = commit.hash
  })

  return CliExitCode.Success
}

export const createSemanticDiffsCommand = createPublicCommandDef({
  action: createSemanticDiffsAction,
  properties: {
    name: 'create-diffs',
    description: 'create semantic diffs from a branch',
    positionalOptions: [
      {
        name: 'repoFolder',
        type: 'string',
        required: true,
      },
      {
        name: 'baseCommit',
        required: true,
        type: 'string',
      },
      {
        name: 'branchName',
        required: true,
        type: 'string',
      },
      {
        name: 'outputFolder',
        required: true,
        type: 'string',
      },
    ],
    keyedOptions: [
      {
        name: 'maxTokens',
        alias: 't',
        type: 'string',
        description: 'max token length of any single diff file',
        default: '3500',
      },
      {
        name: 'preferredFormat',
        alias: 'f',
        type: 'string',
        choices: FormatTypeNames,
      },
      {
        name: 'typesToIgnore',
        alias: 'i',
        type: 'stringsList',
      },
      {
        name: 'regexToFilterOut',
        alias: 'e',
        type: 'stringsList',
      },
    ],
  },
})
