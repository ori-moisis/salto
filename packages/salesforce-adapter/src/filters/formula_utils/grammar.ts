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

import { getField, getObject, parts } from './utils'
import { CPQ_NAMESPACE, CUSTOM_METADATA_SUFFIX, NAMESPACE_SEPARATOR, SALESFORCE_CUSTOM_SUFFIX } from '../../constants'

const RELATIONSHIP_SUFFIX = '__R'
// No need to use the explicit ctor
const USER_FIELDS_REGEX = /^OWNER|MANAGER|CREATEDBY|LASTMODIFIEDBY$/i
// const USER_FIELDS_REGEX = new RegExp(/^OWNER|MANAGER|CREATEDBY|LASTMODIFIEDBY$/, 'i')
const CUSTOM_LABEL_PREFIX_REGEX = new RegExp(/^\$LABEL\./, 'i')
const CUSTOM_SETTING_PREFIX_REGEX = new RegExp(/^\$SETUP\./, 'i')
const OBJECT_TYPE_PREFIX_REGEX = new RegExp(/^\$OBJECTTYPE\./, 'i')
const SELF_REFERENTIAL_PARENT_FIELD_REGEX = new RegExp(/^parentid$/, 'i')
const SELF_REFERENTIAL_PARENT_OBJECT_REGEX = new RegExp(/^parent$/, 'i')
const SPECIAL_PREFIXES_REGEX = new RegExp(/^\$USER|\$PROFILE|\$ORGANIZATION|\$USERROLE|\$SYSTEM/, 'i')

export const isUserField = (value: string): boolean => {
  const prefix = parts(value)[0]
  // Where did this list of names come from?
  return USER_FIELDS_REGEX.test(prefix)
}
// why is some of this using "endsWith" and some using regex?
export const isCustom = (value: string): boolean => value.toLocaleLowerCase().endsWith(SALESFORCE_CUSTOM_SUFFIX)
export const isCustomMetadata = (value: string): boolean => (
  // Why do we allow this on any part? why only this and not the others?
  parts(value.toLocaleLowerCase()).some(part => part.endsWith(CUSTOM_METADATA_SUFFIX))
)
export const isCustomLabel = (value: string): boolean => CUSTOM_LABEL_PREFIX_REGEX.test(value)
export const isCustomSetting = (value: string): boolean => CUSTOM_SETTING_PREFIX_REGEX.test(value)
export const isObjectType = (value: string): boolean => OBJECT_TYPE_PREFIX_REGEX.test(value)
export const isParentField = (value: string): boolean => SELF_REFERENTIAL_PARENT_FIELD_REGEX.test(getField(value))
export const isParent = (value: string): boolean => SELF_REFERENTIAL_PARENT_OBJECT_REGEX.test(value)
export const isStandardRelationship = (value: string): boolean => (
  !value.toLocaleUpperCase().endsWith(RELATIONSHIP_SUFFIX)
)
// Seems like there are a lot of different places that mix the meaning of "." as a separator
// this feels unsafe as we assign different meanings implicitly, this function for example
// isn't really correct in most contexts, there are many strings where having a "." does not mean
// it is a relationship field
export const isRelationshipField = (value: string): boolean => value.includes('.')
export const isSpecialPrefix = (value: string): boolean => SPECIAL_PREFIXES_REGEX.test(value)
export const isProcessBuilderIdentifier = (value: string): boolean => (
  // https://help.salesforce.com/s/articleView?id=000383560&type=1
  value.startsWith('[') && value.endsWith(']')
)
export const isCPQRelationship = (value: string): boolean => {
  const objectUpperCase = getObject(value).toLocaleUpperCase()

  return objectUpperCase.startsWith(`${CPQ_NAMESPACE}${NAMESPACE_SEPARATOR}`)
    && objectUpperCase.endsWith(RELATIONSHIP_SUFFIX)
}
