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
export const chunkBySoft = <T>(
  data: ReadonlyArray<T>,
  hardLimit: number,
  lengthFunc: (item: T) => number,
): Array<Array<T>> => {
  // Try to distribute the tokens evenly among chunks, this prevents us from having a very small commit where the model
  // will tend to provide smaller details which we don't really want when we merge all the descriptions together
  const totalTokens = data.reduce((sum, change) => sum + lengthFunc(change), 0)
  const numGroups = Math.ceil(totalTokens / hardLimit)
  const softLimit = Math.ceil(totalTokens / numGroups)

  const outputChunks = []
  let currChunk: Array<T> = []
  let currLen = 0
  data.forEach(item => {
    const itemLen = lengthFunc(item)
    if ((currLen + itemLen) > hardLimit && currChunk.length > 0) {
      outputChunks.push(currChunk)
      currLen = 0
      currChunk = []
    }
    currLen += itemLen
    currChunk.push(item)
    if (currLen > softLimit) {
      outputChunks.push(currChunk)
      currLen = 0
      currChunk = []
    }
  })
  if (currChunk.length > 0) {
    outputChunks.push(currChunk)
  }
  return outputChunks
}
