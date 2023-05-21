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
import { logger } from '@salto-io/logging'
import { SimpleGit } from 'simple-git'

const log = logger(module)

export const resetBranchToCommit = async (git: SimpleGit, branch: string, base: string): Promise<void> => {
  const currentBranches = await git.branchLocal()
  if (currentBranches.all.includes(branch)) {
    log.debug('Checking out %s', branch)
    await git.checkout(branch)
    log.debug('hard reset to %s', base)
    await git.reset(['--hard', base])
  } else {
    log.debug('checkout new branch %s onto %s', branch, base)
    await git.checkoutBranch(branch, base)
  }
}
