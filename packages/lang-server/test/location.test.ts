/*
*                      Copyright 2020 Salto Labs Ltd.
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
import * as path from 'path'
import { EditorWorkspace } from '../src/workspace'
import { getQueryLocations } from '../src/location'
import { mockWorkspace } from './workspace'

// eslint-disable-next-line jest/no-disabled-tests
describe('workspace query locations', () => {
  let workspace: EditorWorkspace
  const baseDir = path.resolve(`${__dirname}/../../test/test-nacls/`)
  const naclFileName = path.join(baseDir, 'all.nacl')

  beforeAll(async () => {
    workspace = new EditorWorkspace(baseDir, await mockWorkspace(naclFileName))
  })

  it('should find prefixes', async () => {
    const res = await getQueryLocations(workspace, 'vs.per')
    expect(res).toHaveLength(7)
    expect(res[0].fullname).toBe('vs.person')
  })
  it('should find suffixes', async () => {
    const res = await getQueryLocations(workspace, 's.person')
    expect(res).toHaveLength(2)
    expect(res[0].fullname).toBe('vs.person')
  })
  it('should find fragments in last name part', async () => {
    const res = await getQueryLocations(workspace, 'erso')
    expect(res).toHaveLength(2)
    expect(res[0].fullname).toBe('vs.person')
  })
  it('should  return empty results on not found', async () => {
    const res = await getQueryLocations(workspace, 'nope')
    expect(res).toHaveLength(0)
  })
})
