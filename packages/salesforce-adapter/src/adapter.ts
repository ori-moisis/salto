import {
  BuiltinTypes, Type, ObjectType, ElemID, InstanceElement, Values,
  Field, Element, isObjectType, isInstanceElement, isPrimitiveType,
} from 'adapter-api'
import {
  SaveResult, ValueTypeField, MetadataInfo, Field as SObjField,
} from 'jsforce'
import { isArray } from 'util'
import _ from 'lodash'
import SalesforceClient from './client/client'
import * as constants from './constants'
import {
  CompleteSaveResult, SfError,
} from './client/types'
import {
  toCustomField, toCustomObject, apiName, sfCase, fieldFullName, Types,
  getValueTypeFieldElement, getSObjectFieldElement, fromMetadataInfo,
  bpCase, toMetadataInfo, metadataType,
} from './transformer'
import { filter as layoutFilter } from './filters/layouts'
import { filter as fieldPermissionsFilter } from './filters/field_permissions'
import { filter as validationRulesFilter } from './filters/validation_rules'
import { filter as assignmentRulesFilter } from './filters/assignment_rules'
import convertListsFilter from './filters/convert_lists'
import convertTypeFilter from './filters/convert_types'
import missingFieldsFilter from './filters/missing_fields'
import Filter from './filters/filter'

// Diagnose client results
const diagnose = (result: SaveResult | SaveResult[]): void => {
  const errorMessage = (error: SfError | SfError[]): string => {
    if (isArray(error)) {
      return error.map(e => e.message).join('\n')
    }
    return error.message
  }

  if (!result) {
    return
  }
  let errors: string[] = []
  if (isArray(result)) {
    errors = errors.concat(
      (result as CompleteSaveResult[])
        .filter(r => r && r.errors)
        .map(r => errorMessage(r.errors))
    )
  } else if ((result as CompleteSaveResult).errors) {
    errors.push(errorMessage((result as CompleteSaveResult).errors))
  }

  if (errors.length > 0) {
    // TODO: use CrudError
    throw Error(errors.join('\n'))
  }
}

// Add API name and label annotation if missing
const annotateApiNameAndLabel = (element: ObjectType): void => {
  const innerAnnotate = (annotations: Values, name: string): void => {
    if (!annotations[constants.API_NAME]) {
      annotations[constants.API_NAME] = sfCase(name, true)
    }
    if (!annotations[constants.LABEL]) {
      annotations[constants.LABEL] = sfCase(name)
    }
  }

  element.annotate({ [constants.METADATA_TYPE]: constants.CUSTOM_OBJECT })
  innerAnnotate(element.getAnnotationsValues(), element.elemID.name)
  Object.values(element.fields).forEach(field => {
    innerAnnotate(field.getAnnotationsValues(), field.name)
  })
}

const validateApiName = (prevElement: Element, newElement: Element): void => {
  if (apiName(prevElement) !== apiName(newElement)) {
    throw Error(
      `Failed to update element as api names prev=${apiName(
        prevElement
      )} and new=${apiName(newElement)} are different`
    )
  }
}

interface SalesforceAdapterParams {
  // Metadata types that we want to treat as top level types (discover instances of them)
  // even though they are not returned as top level metadata types from the API
  metadataAdditionalTypes?: string[]

  // Metadata types that we do not want to discover even though they are returned as top level
  // types from the API
  metadataTypeBlacklist?: string[]

  // Filters to apply to all adapter operations
  filters?: Filter[]
}

export default class SalesforceAdapter {
  private metadataAdditionalTypes: string[]
  private metadataTypeBlacklist: string[]
  private filters: Filter[]

  public constructor({
    metadataAdditionalTypes = [
      'ValidationRule', // This is a subtype of CustomObject
    ],
    metadataTypeBlacklist = [
      'ApexClass', // For some reason we cannot access this from the metadata API
      'InstalledPackage', // Instances of this don't actually have an ID and they contain duplicates
      'CustomObject', // We have special treatment for this type
    ],
    filters = [
      fieldPermissionsFilter,
      layoutFilter,
      validationRulesFilter,
      assignmentRulesFilter,
      missingFieldsFilter,
      // The following filters should remain last in order to make sure they fix all elements
      convertListsFilter,
      convertTypeFilter,
    ],
  }: SalesforceAdapterParams = {}) {
    this.metadataAdditionalTypes = metadataAdditionalTypes
    this.metadataTypeBlacklist = metadataTypeBlacklist
    this.filters = filters
  }

  private innerClient?: SalesforceClient
  public get client(): SalesforceClient {
    return this.innerClient as SalesforceClient
  }

  init(conf: InstanceElement): void {
    this.innerClient = new SalesforceClient(
      conf.value.username,
      conf.value.password + conf.value.token,
      conf.value.sandbox
    )
  }

  /**
   * @return {ObjectType} - The configuration type for the adapter.
   * This is used by core to:
   * 1) Locate the proper configuration type for the adapter,
   * 2) Prompt the user in order to create an instance of it if it can't
   *    find it in the blueprints
   */
  // disable class method use as we need this function for Adapter interface
  // eslint-disable-next-line class-methods-use-this
  public getConfigType(): ObjectType {
    const configID = new ElemID('salesforce')
    const config = new ObjectType({
      elemID: configID,
      fields: {
        username: new Field(configID, 'username', BuiltinTypes.STRING),
        password: new Field(configID, 'password', BuiltinTypes.STRING),
        token: new Field(configID, 'token', BuiltinTypes.STRING),
        sandbox: new Field(configID, 'sandbox', BuiltinTypes.BOOLEAN),
      },
      annotations: {},
      annotationsValues: {},
    })

    return config
  }

  /**
   * Discover configuration elements (types and instances in the given salesforce account)
   * Account credentials were given in the constructor.
   */
  public async discover(): Promise<Element[]> {
    const fieldTypes = Types.getAllFieldTypes()
    const metadataTypeNames = this.client.listMetadataTypes().then(
      types => types
        .map(x => x.xmlName)
        .concat(this.metadataAdditionalTypes)
    )
    const metadataTypes = this.discoverMetadataTypes(metadataTypeNames)
    const metadataInstances = this.discoverMetadataInstances(metadataTypeNames, metadataTypes)

    // Filter out types returned as both metadata types and SObjects
    const sObjects = this.discoverSObjects().then(
      async types => {
        // All metadata type names include subtypes as well as the "top level" type names
        const allMetadataTypeNames = new Set((await metadataTypes).map(elem => elem.elemID.name))
        return types.filter(t => !allMetadataTypeNames.has(t.elemID.name))
      }
    )

    const elements = _.flatten(
      await Promise.all([fieldTypes, metadataTypes, sObjects, metadataInstances]) as Element[][]
    )

    await this.runFiltersOnDiscover(elements)
    return elements
  }

  /**
   * Add new element
   * @param element the object/instance to add
   * @returns the updated element with extra info like api name, label and metadata type
   * @throws error in case of failure
   */
  public async add(element: Element): Promise<Element> {
    let post: Element
    if (isObjectType(element)) {
      post = await this.addObject(element)
    } else {
      post = await this.addInstance(element as InstanceElement)
    }
    diagnose(await this.runFiltersOnAdd(post))

    return post
  }

  /**
   * Add new object
   * @param element of ObjectType to add
   * @returns the updated object with extra info like api name, label and metadata type
   * @throws error in case of failure
   */
  private async addObject(element: ObjectType): Promise<ObjectType> {
    const post = element.clone()
    annotateApiNameAndLabel(post)

    diagnose(await this.client.create(constants.CUSTOM_OBJECT, toCustomObject(post)))

    return post
  }

  /**
   * Add new Instance
   * @param instance to add
   * @returns the updated instance
   * @throws error in case of failure
   */
  private async addInstance(element: InstanceElement): Promise<InstanceElement> {
    const result = await this.client.create(
      metadataType(element),
      toMetadataInfo(apiName(element), element.value)
    )
    diagnose(result)

    return element
  }

  /**
   * Remove an element (object/instance)
   * @param element to remove
   */
  public async remove(element: Element): Promise<void> {
    diagnose(await this.client.delete(metadataType(element), apiName(element)))
    diagnose(await this.runFiltersOnRemove(element))
  }

  /**
   * Updates an Element
   * @param prevElement The metadata of the old element
   * @param newElement The new metadata of the element to replace
   * @returns the updated element
   */
  public async update(prevElement: Element, newElement: Element): Promise<Element> {
    if (isObjectType(prevElement) && isObjectType(newElement)) {
      return this.updateObject(prevElement, newElement)
    }

    if (isInstanceElement(prevElement) && isInstanceElement(newElement)) {
      return this.updateInstance(prevElement, newElement)
    }

    return newElement
  }

  /**
   * Update a custom object
   * @param prevObject The metadata of the old object
   * @param newObject The new metadata of the object to replace
   * @returns the updated object
   */
  private async updateObject(prevObject: ObjectType, newObject: ObjectType): Promise<ObjectType> {
    const clonedObject = newObject.clone()
    annotateApiNameAndLabel(clonedObject)

    validateApiName(prevObject, clonedObject)

    const pre = prevObject.clone()
    annotateApiNameAndLabel(pre)

    const fieldsUpdateResult = await Promise.all([
      // Retrieve the custom fields for deletion and delete them
      this.deleteCustomFields(prevObject, prevObject.getFieldsThatAreNotInOther(clonedObject)),
      // Retrieve the custom fields for addition and than create them
      this.createFields(clonedObject, clonedObject.getFieldsThatAreNotInOther(prevObject)),
      // Update the remaining fields that were changed
      this.updateFields(clonedObject,
        clonedObject.getMutualFieldsWithOther(pre).filter(afterField =>
          !_.isEqual(afterField.getAnnotationsValues(),
            pre.fields[afterField.name].getAnnotationsValues()))),
    ])
    // Update the annotation values - this can't be done asynchronously with the previous
    // operations because the update API expects to receive the updated list of fields,
    // hence the need to perform the fields deletion and creation first, and then update the
    // object.
    // IMPORTANT: We don't update a built-in object (such as Lead, Customer, etc.)
    // The update API currently allows us to add/remove custom fields to such objects, but not
    // to update them.
    let objectUpdateResult: SaveResult | SaveResult[] = []
    if (apiName(clonedObject).endsWith(constants.SALESFORCE_CUSTOM_SUFFIX)
      // Don't update the object unless its annotations values have changed
      && !_.isEqual(pre.getAnnotationsValues(), clonedObject.getAnnotationsValues())) {
      objectUpdateResult = await this.client.update(
        metadataType(clonedObject),
        toCustomObject(clonedObject, false)
      ) // Update the object without its fields
    }

    // Aspects should be updated once all object related properties updates are over
    const filtersResult = await this.runFiltersOnUpdate(prevObject, clonedObject)
    diagnose([..._.flatten(fieldsUpdateResult), objectUpdateResult as SaveResult,
      ...filtersResult])

    return clonedObject
  }

  /**
   * Update an instance
   * @param prevInstance The metadata of the old instance
   * @param newInstance The new metadata of the instance to replace
   * @returns the updated instance
   */
  private async updateInstance(prevInstance: InstanceElement, newInstance: InstanceElement):
    Promise<InstanceElement> {
    validateApiName(prevInstance, newInstance)

    const instanceUpdateResult = await this.client.update(
      metadataType(newInstance),
      toMetadataInfo(
        apiName(newInstance),
        newInstance.getValuesThatNotInPrevOrDifferent(prevInstance.value)
      )
    )

    diagnose(instanceUpdateResult)
    return newInstance
  }

  /**
   * Updates custom fields
   * @param object the object that the fields belong to
   * @param fieldsToUpdate The fields to update
   * @returns successfully managed to update all fields
   */
  private async updateFields(object: ObjectType, fieldsToUpdate: Field[]): Promise<SaveResult[]> {
    if (fieldsToUpdate.length === 0) return []
    // Update the custom fields
    return _.flatten(await Promise.all(_.chunk(fieldsToUpdate, 10).map(chunk => this.client.update(
      constants.CUSTOM_FIELD,
      chunk.map(f => toCustomField(object, f, true))
    ) as Promise<SaveResult[]>)))
  }

  /**
   * Creates custom fields and their corresponding field permissions
   * @param object the object that the fields belong to
   * @param fieldsToAdd The fields to create
   * @returns successfully managed to create all fields with their permissions or not
   */
  private async createFields(object: ObjectType, fieldsToAdd: Field[]): Promise<SaveResult[]> {
    if (fieldsToAdd.length === 0) return []
    // Create the custom fields
    return _.flatten(await Promise.all(_.chunk(fieldsToAdd, 10).map(chunk => this.client.create(
      constants.CUSTOM_FIELD,
      chunk.map(f => toCustomField(object, f, true))
    ) as Promise<SaveResult[]>)))
  }

  /**
   * Deletes custom fields
   * @param element the object api name those fields reside in
   * @param fields the custom fields we wish to delete
   */
  private async deleteCustomFields(element: ObjectType, fields: Field[]): Promise<SaveResult[]> {
    if (fields.length === 0) return []
    return _.flatten(await Promise.all(_.chunk(fields, 10).map(chunk => this.client.delete(
      constants.CUSTOM_FIELD,
      chunk.map(field => fieldFullName(element, field))
    ) as Promise<SaveResult[]>)))
  }

  private async discoverMetadataTypes(typeNames: Promise<string[]>): Promise<Type[]> {
    const knownTypes = new Map<string, Type>()
    return _.flatten(await Promise.all((await typeNames)
      .filter(name => !this.metadataTypeBlacklist.includes(name))
      .map(obj => this.discoverMetadataType(obj, knownTypes))))
  }

  private async discoverMetadataType(objectName: string, knownTypes: Map<string, Type>):
    Promise<Type[]> {
    const fields = await this.client.describeMetadataType(objectName)
    return SalesforceAdapter.createMetadataTypeElements(objectName, fields, knownTypes)
  }

  private static createMetadataTypeElements(
    objectName: string,
    fields: ValueTypeField[],
    knownTypes: Map<string, Type>,
    isSubtype = false,
  ): Type[] {
    if (knownTypes.has(objectName)) {
      // Already created this type, no new types to return here
      return []
    }
    const element = Types.get(objectName, false) as ObjectType
    knownTypes.set(objectName, element)
    element.annotate({ [constants.METADATA_TYPE]: objectName })
    element.path = ['types', ...(isSubtype ? ['subtypes'] : []), element.elemID.name]
    if (!fields) {
      return [element]
    }

    // We need to create embedded types BEFORE creating this element's fields
    // in order to make sure all internal types we may need are updated in the
    // knownTypes map
    const embeddedTypes = _.flatten(fields.filter(field => !_.isEmpty(field.fields)).map(
      field => this.createMetadataTypeElements(
        field.soapType,
        Array.isArray(field.fields) ? field.fields : [field.fields],
        knownTypes,
        true,
      )
    ))

    // Enum fields sometimes show up with a type name that is not primitive but also does not
    // have fields (so we won't create an embedded type for it). it seems like these "empty" types
    // are always supposed to be a string with some restriction so we map all non primitive "empty"
    // types to string
    fields
      .filter(field => _.isEmpty(field.fields))
      .filter(field => !isPrimitiveType(Types.get(field.soapType, false)))
      .forEach(field => knownTypes.set(field.soapType, BuiltinTypes.STRING))

    const fieldElements = fields.map(field =>
      getValueTypeFieldElement(element.elemID, field, knownTypes))

    // Set fields on elements
    fieldElements.forEach(field => {
      element.fields[field.name] = field
    })

    return _.flatten([element, embeddedTypes])
  }

  private async discoverMetadataInstances(typeNames: Promise<string[]>, types: Promise<Type[]>):
    Promise<InstanceElement[]> {
    const topLevelTypeNames = await typeNames
    const instances = await Promise.all((await types)
      .filter(isObjectType)
      .filter(t => topLevelTypeNames.includes(sfCase(t.elemID.name)))
      .map(t => this.createInstanceElements(t)))
    return _.flatten(instances)
  }

  private async createInstanceElements(type: ObjectType): Promise<InstanceElement[]> {
    const typeName = sfCase(type.elemID.name)
    const isSettings = typeName === constants.SETTINGS_METADATA_TYPE
    const instances = await this.listMetadataInstances(typeName)
    return instances.filter(i => i.fullName !== undefined)
      .map(i => new InstanceElement(
        new ElemID(constants.SALESFORCE, type.elemID.name, bpCase(i.fullName)),
        type,
        fromMetadataInfo(i),
        isSettings ? ['settings'] : ['records', type.elemID.name, bpCase(i.fullName)],
      ))
  }

  private async discoverSObjects(): Promise<Type[]> {
    const customObjectNames = this.client.listMetadataObjects(constants.CUSTOM_OBJECT).then(
      objs => new Set(objs.map(obj => obj.fullName))
    )

    return _.flatten(await Promise.all(_.flatten(await Promise.all(
      _(await this.client.listSObjects())
        .map(sobj => sobj.name)
        .chunk(100)
        .map(nameChunk => this.client.describeSObjects(nameChunk))
        .map(async objects => (await objects).map(async ({ name, custom, fields }) =>
          SalesforceAdapter.createSObjectTypes(name, custom, fields, await customObjectNames)))
        .value()
    ))))
  }

  private static createSObjectTypes(
    objectName: string,
    isCustom: boolean,
    fields: SObjField[],
    customObjectNames: Set<string>,
  ): Type[] {
    const element = Types.get(objectName) as ObjectType
    element.annotate({ [constants.API_NAME]: objectName })
    element.annotate({ [constants.METADATA_TYPE]: constants.CUSTOM_OBJECT })

    // Set standard fields on element
    fields
      .filter(f => !f.custom)
      .map(f => getSObjectFieldElement(element.elemID, f))
      .forEach(field => {
        element.fields[field.name] = field
      })

    // Create custom fields (if any)
    const customFields = fields
      .filter(f => f.custom)
      .map(f => getSObjectFieldElement(element.elemID, f))

    if (!customObjectNames.has(objectName)) {
      // This is not a custom object type, no need to separate standard part from custom part
      customFields.forEach(field => {
        element.fields[field.name] = field
      })
      element.path = ['types', 'object', element.elemID.name]
      return [element]
    }

    if (isCustom) {
      // This is custom object, we treat standard fields as if they were custom as well
      // so we put all fields in the same element definition
      customFields.forEach(field => {
        element.fields[field.name] = field
      })
      element.path = ['objects', 'custom', element.elemID.name]
      return [element]
    }

    // This is a standard object
    element.path = ['objects', 'standard', element.elemID.name]

    if (_.isEmpty(customFields)) {
      // No custom parts, only standard element needed
      return [element]
    }

    // Custom fields go in a separate element
    const customPart = Types.get(objectName) as ObjectType
    customFields.forEach(field => {
      customPart.fields[field.name] = field
    })
    customPart.path = ['objects', 'custom', customPart.elemID.name]
    return [element, customPart]
  }

  /**
   * List all the instances of specific metadataType
   * @param type the metadata type
   */
  private async listMetadataInstances(type: string): Promise<MetadataInfo[]> {
    const objs = await this.client.listMetadataObjects(type)
    if (!objs) {
      return []
    }
    const names = objs.map(obj => obj.fullName)
    // For some unknown reason, for metadata type = 'Settings', when calling readMetadata we should
    // use type = OBJNAME+'Settings'
    if (type === constants.SETTINGS_METADATA_TYPE) {
      return Promise.all(names
        .map(name => this.client.readMetadata(name + type, name) as Promise<MetadataInfo>))
    }
    return _.flatten(await Promise.all(_.chunk(names, 10)
      .map(chunk => this.client.readMetadata(type, chunk) as Promise<MetadataInfo[]>)))
  }

  // Filter related functions
  private async runFiltersOnDiscover(elements: Element[]): Promise<void> {
    // Discover filters order is important so they should run one after the other
    return this.filters.reduce(
      (prevRes, filter) => prevRes.then(() => filter.onDiscover(this.client, elements)),
      Promise.resolve(),
    )
  }

  private async runFiltersOnAdd(after: Element): Promise<SaveResult[]> {
    return _.flatten(await Promise.all(this.filters.map(filter =>
      filter.onAdd(this.client, after))))
  }


  private async runFiltersOnUpdate(before: Element, after: Element): Promise<SaveResult[]> {
    return _.flatten(await Promise.all(this.filters.map(filter =>
      filter.onUpdate(this.client, before, after))))
  }

  private async runFiltersOnRemove(before: Element): Promise<SaveResult[]> {
    return _.flatten(await Promise.all(this.filters.map(filter =>
      filter.onRemove(this.client, before))))
  }
}
