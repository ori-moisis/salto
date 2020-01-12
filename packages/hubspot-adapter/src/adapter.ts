import _ from 'lodash'
import {
  isInstanceElement,
  Element, InstanceElement, ObjectType,
} from 'adapter-api'
import {
  Form,
} from './client/types'
import HubspotClient from './client/client'
import {
  Types, createHubspotInstanceElement, fromHubspotObject,
} from './transformers/transformer'

const validateFormGuid = (
  before: InstanceElement,
  after: InstanceElement
): void => {
  if (before.value.guid !== after.value.guid) {
    throw Error(
      `Failed to update element as guid's prev=${
        before.value.guid
      } and new=${after.value.guid} are different`
    )
  }
}

export interface HubspotAdapterParams {
  // client to use
  client: HubspotClient

}

export default class HubspotAdapter {
  private client: HubspotClient

  public constructor({
    client,
  }: HubspotAdapterParams) {
    this.client = client
  }

  /**
   * Fetch configuration elements: objects, types and instances for the given HubSpot account.
   * Account credentials were given in the constructor.
   */
  public async fetch(): Promise<Element[]> {
    const fieldTypes = Types.getAllFieldTypes()
    const objects = Types.hubspotObjects
    const subTypes = Types.hubspotSubTypes
    const instances = await this.fetchHubInstances(objects)

    return _.flatten(
      [fieldTypes, objects, subTypes, instances] as Element[][]
    )
  }

  private async fetchHubInstances(
    types: ObjectType[]
  ): Promise<InstanceElement[]> {
    const instances = await Promise.all((types)
      .map(t => this.fetchHubspotInstances(t)))
    return _.flatten(instances)
  }

  private async fetchHubspotInstances(type: ObjectType): Promise<InstanceElement[]> {
    const instances = await this.client.getAllForms()
    return instances
      .map(i => createHubspotInstanceElement(i, type))
  }


  /**
   * Add new instance
   * Hubspot API support only instances additions
   * @param instance the instance to add
   * @returns the updated element
   * @throws error in case of failure
   */
  public async add(instance: InstanceElement): Promise<InstanceElement> {
    const post = instance.clone()
    const resp = await this.client.createForm(post.value as Form)

    // Copy the response values (adding autogenerated fields and removing unsupported fields)
    post.value = fromHubspotObject(resp, post.type)
    return post
  }

  /**
   * Remove an instance
   * @param instance to remove
   * @throws error in case of failure
   */
  public async remove(instance: InstanceElement): Promise<void> {
    await this.client.deleteForm(
      {
        guid: instance.value.guid,
      } as Form
    )
  }

  /**
   * Updates an Element
   * @param before The metadata of the old element
   * @param after The new metadata of the element to replace
   * @returns the updated element
   */
  public async update(
    before: Element,
    after: Element,
  ): Promise<Element> {
    if (isInstanceElement(before) && isInstanceElement(after)) {
      validateFormGuid(before, after)
      await this.client.updateForm(
        {
          guid: after.value.guid,
        } as Form
      )
    }

    return after
  }
}