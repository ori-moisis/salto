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
import { Configuration, OpenAIApi } from 'openai'
import Bottleneck from 'bottleneck'
import { inspect } from 'util'
import { collections } from '@salto-io/lowerdash'
import { logger } from '@salto-io/logging'

const { awu } = collections.asynciterable
const log = logger(module)

// TODO: make this not global
const configuration = new Configuration({ apiKey: process.env.OPENAI_API_KEY })
const openai = new OpenAIApi(configuration)
const clientQueue = new Bottleneck({ maxConcurrent: 2 })

const MAX_ATTEMPTS = 3
const INITIAL_DELAY_SECONDS = 2

const generateCompletion = (systemPrompt: string, userPrompt: string, maxTokens: number): Promise<string> => log.time(
  async () => {
    const runCompletionWithBackOff = clientQueue.wrap(
      async (attempt: number): Promise<ReturnType<typeof openai.createChatCompletion>> => {
        try {
          log.debug('Sending completion request attempt %d', attempt)
          return await openai.createChatCompletion({
            model: 'gpt-3.5-turbo',
            messages: [
              { role: 'system', content: systemPrompt },
              { role: 'user', content: userPrompt },
            ],
            max_tokens: maxTokens,
            temperature: 0,
          })
        } catch (e) {
          if (attempt >= MAX_ATTEMPTS) {
            throw e
          }
          const errorJson = e?.toJSON() ?? inspect(e)
          log.warn('Completion request failed with error %s, delaying for %d seconds', errorJson, INITIAL_DELAY_SECONDS ** attempt)
          await new Promise(resolve => setTimeout(resolve, (INITIAL_DELAY_SECONDS ** attempt) * 1000))
          return runCompletionWithBackOff(attempt + 1)
        }
      }
    )
    const completion = await runCompletionWithBackOff(1)
    return completion.data.choices[0].message?.content ?? ''
  },
  'Generate completion from openai',
)

type GetCommitMessageForChangesArgs = {
  changes: string[]
  promptForCommit: string
  promptForMerge: string
  maxTokens: number
}
export const getCommitMessageForChanges = async (
  { changes, promptForCommit, promptForMerge, maxTokens }: GetCommitMessageForChangesArgs
): Promise<string> => {
  const commitMessages = await awu(changes)
    .map(chunk => generateCompletion(chunk, promptForCommit, maxTokens))
    .toArray()
  if (commitMessages.length === 1) {
    return commitMessages[0]
  }

  return generateCompletion(commitMessages.join('\n\n'), promptForMerge, maxTokens)
}
