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
import {
  AuthenticationService, CLIConfigurationService, CommandActionExecutor, CommandInstanceFactory,
  CommandOptionsValidator, CommandOutputHandler, CommandsMetadataService, SDKOperationResultUtils,
  OperationResult,
} from '@salto-io/suitecloud-cli'
import { decorators, hash } from '@salto-io/lowerdash'
import { Values, AccountId } from '@salto-io/adapter-api'
import { readDir, readFile, writeFile } from '@salto-io/file'
import { compareLogLevels, logger } from '@salto-io/logging'
import xmlParser from 'fast-xml-parser'
import path from 'path'
import os from 'os'

const log = logger(module)

export type Credentials = {
  accountId: string
  tokenId: string
  tokenSecret: string
}

export type NetsuiteClientOpts = {
  credentials: Credentials
}

export const COMMANDS = {
  CREATE_PROJECT: 'project:create',
  SETUP_ACCOUNT: 'account:setup',
  IMPORT_OBJECTS: 'object:import',
  DEPLOY_PROJECT: 'project:deploy',
  ADD_PROJECT_DEPENDENCIES: 'project:adddependencies',
}

export const ATTRIBUTE_PREFIX = '@_'
export const CDATA_TAG_NAME = '__cdata'

const OBJECTS_DIR = 'Objects'
const SRC_DIR = 'src'

const rootCLIPath = path.normalize(path.join(__dirname, ...Array(5).fill('..'), 'node_modules',
  '@salto-io', 'suitecloud-cli', 'src'))
const baseExecutionPath = os.tmpdir()

export interface CustomizationInfo {
  typeName: string
  values: Values
}

export const convertToCustomizationInfo = (xmlContent: string): CustomizationInfo => {
  const parsedXmlValues = xmlParser.parse(xmlContent,
    { attributeNamePrefix: ATTRIBUTE_PREFIX, ignoreAttributes: false })
  const typeName = Object.keys(parsedXmlValues)[0]
  return { typeName, values: parsedXmlValues[typeName] }
}

export const convertToXmlContent = (customizationInfo: CustomizationInfo): string =>
  // eslint-disable-next-line new-cap
  new xmlParser.j2xParser({
    attributeNamePrefix: ATTRIBUTE_PREFIX,
    // We convert to an not formatted xml since the CDATA transformation is wrong when having format
    format: false,
    ignoreAttributes: false,
    cdataTagName: CDATA_TAG_NAME,
  }).parse({ [customizationInfo.typeName]: customizationInfo.values })

const setSdfLogLevel = (): void => {
  const isSaltoLogVerbose = (): boolean => {
    const saltoLogLevel = logger.config.minLevel
    return saltoLogLevel !== 'none' && compareLogLevels(saltoLogLevel, 'debug') >= 0
  }
  process.env.IS_SDF_VERBOSE = isSaltoLogVerbose() ? 'true' : 'false'
}

export default class NetsuiteClient {
  private projectName?: string
  private projectCommandActionExecutor?: CommandActionExecutor
  private isAccountSetUp = false
  private readonly credentials: Credentials
  private readonly authId: string

  constructor({ credentials }: NetsuiteClientOpts) {
    this.credentials = credentials
    this.authId = hash.toMD5(this.credentials.tokenId)
    setSdfLogLevel()
  }

  static async validateCredentials(credentials: Credentials): Promise<AccountId> {
    const netsuiteClient = new NetsuiteClient({ credentials })
    await netsuiteClient.setupAccount()
    return Promise.resolve(credentials.accountId)
  }

  private static initCommandActionExecutor(executionPath: string): CommandActionExecutor {
    const commandsMetadataService = new CommandsMetadataService(rootCLIPath)
    commandsMetadataService.initializeCommandsMetadata()
    return new CommandActionExecutor({
      executionPath,
      commandOutputHandler: new CommandOutputHandler(),
      commandOptionsValidator: new CommandOptionsValidator(),
      cliConfigurationService: new CLIConfigurationService(),
      commandInstanceFactory: new CommandInstanceFactory(),
      authenticationService: new AuthenticationService(executionPath),
      commandsMetadataService,
    })
  }

  private async ensureAccountIsSetUp(): Promise<void> {
    if (!this.isAccountSetUp) {
      await this.setupAccount()
      this.isAccountSetUp = true
    }
  }

  private static requiresSetupAccount = decorators.wrapMethodWith(
    async function withSetupAccount(
      this: NetsuiteClient,
      originalMethod: decorators.OriginalCall
    ): Promise<unknown> {
      await this.ensureAccountIsSetUp()
      return originalMethod.call()
    }
  )

  private static logDecorator = decorators.wrapMethodWith(
    async (
      { call, name }: decorators.OriginalCall,
    ): Promise<unknown> => {
      const desc = `client.${name}`
      try {
        return await log.time(call, desc)
      } catch (e) {
        log.error('failed to run Netsuite client command on: %o', e)
        throw e
      }
    }
  )

  private static async createProject(): Promise<string> {
    const projectName = `TempProject${String(Date.now()).substring(8)}`
    const operationResult = await NetsuiteClient.initCommandActionExecutor(baseExecutionPath)
      .executeAction({
        commandName: COMMANDS.CREATE_PROJECT,
        runInInteractiveMode: false,
        arguments: {
          projectname: projectName,
          type: 'ACCOUNTCUSTOMIZATION',
          parentdirectory: rootCLIPath,
        },
      })
    NetsuiteClient.verifySuccessfulOperation(operationResult)
    return projectName
  }

  private static verifySuccessfulOperation(operationResult: OperationResult): void {
    if (SDKOperationResultUtils.hasErrors(operationResult)) {
      throw Error(SDKOperationResultUtils.getErrorMessagesString(operationResult))
    }
  }

  private async executeProjectAction(commandName: string, commandArguments: Values): Promise<void> {
    if (!this.projectName) {
      this.projectName = await NetsuiteClient.createProject()
    }

    if (!this.projectCommandActionExecutor) {
      this.projectCommandActionExecutor = NetsuiteClient
        .initCommandActionExecutor(`${this.getProjectPath()}`)
    }

    const operationResult = await this.projectCommandActionExecutor.executeAction({
      commandName,
      runInInteractiveMode: false,
      arguments: commandArguments,
    })

    NetsuiteClient.verifySuccessfulOperation(operationResult)
  }

  protected async setupAccount(): Promise<void> {
    // Todo: use the correct implementation and not Salto's temporary solution after:
    //  https://github.com/oracle/netsuite-suitecloud-sdk/issues/81 is resolved
    const setupAccountUsingExistingAuthID = async (): Promise<void> =>
      this.executeProjectAction(COMMANDS.SETUP_ACCOUNT, {
        authid: this.authId,
      })

    const setupAccountUsingNewAuthID = async (): Promise<void> =>
      this.executeProjectAction(COMMANDS.SETUP_ACCOUNT, {
        authid: this.authId,
        accountid: this.credentials.accountId,
        tokenid: this.credentials.tokenId,
        tokensecret: this.credentials.tokenSecret,
      })

    try {
      await setupAccountUsingExistingAuthID()
    } catch (e) {
      await setupAccountUsingNewAuthID()
    }
  }

  @NetsuiteClient.logDecorator
  @NetsuiteClient.requiresSetupAccount
  async listCustomObjects(): Promise<CustomizationInfo[]> {
    await this.executeProjectAction(COMMANDS.IMPORT_OBJECTS, {
      destinationfolder: `${path.sep}${OBJECTS_DIR}`,
      type: 'ALL',
      scriptid: 'ALL',
      excludefiles: true,
    })

    const objectsDirPath = this.getObjectsDirPath()
    const dirContent = await readDir(objectsDirPath)
    // Todo: when we'll support more types (e.g. emailTemplates), there might be other file types
    //  in the directory. remove the below row once these types are supported
    const xmlFilesInDir = dirContent.filter(filename => filename.endsWith('xml'))
    return Promise.all(xmlFilesInDir.map(async filename => {
      const xmlContent = await readFile(path.resolve(objectsDirPath, filename))
      return convertToCustomizationInfo(xmlContent.toString())
    }))
  }

  private getObjectsDirPath(): string {
    return path.resolve(this.getProjectPath(), SRC_DIR, OBJECTS_DIR)
  }

  private getProjectPath(): string {
    return path.resolve(baseExecutionPath, this.projectName as string)
  }

  @NetsuiteClient.logDecorator
  @NetsuiteClient.requiresSetupAccount
  async deployCustomObject(filename: string, customizationInfo: CustomizationInfo): Promise<void> {
    await this.deploy(path.resolve(this.getObjectsDirPath(), `${filename}.xml`), customizationInfo)
  }

  private async deploy(filePath: string, customizationInfo: CustomizationInfo): Promise<void> {
    await writeFile(filePath, convertToXmlContent(customizationInfo))
    await this.addProjectDependencies()
    await this.executeProjectAction(COMMANDS.DEPLOY_PROJECT, {})
  }

  private async addProjectDependencies(): Promise<void> {
    return this.executeProjectAction(COMMANDS.ADD_PROJECT_DEPENDENCIES, {})
  }
}
