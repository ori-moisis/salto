/*
*                      Copyright 2021 Salto Labs Ltd.
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
/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/no-unused-vars */
import sourceMapSupport from 'source-map-support'
import _ from 'lodash'
import { promises, collections } from '@salto-io/lowerdash'
import { loadLocalWorkspace } from '@salto-io/core'
import neo4j, { QueryResult } from 'neo4j-driver'
import yargs from 'yargs'
import { isObjectType, Element, isType, InstanceElement, isInstanceElement, Field, isField, INSTANCE_ANNOTATIONS, ReferenceExpression, isReferenceExpression, isListType, ElemID, isPrimitiveValue, isPrimitiveType, BuiltinTypes } from '@salto-io/adapter-api'
import { transformElement, TransformFunc } from '@salto-io/adapter-utils'

sourceMapSupport.install()

type DBClient = {
  clear: () => Promise<void>
  createTypes: (elements: ReadonlyArray<Element>) => Promise<void>
  createRelationships: (elements: ReadonlyArray<Element>) => Promise<void>
}


const DbClasses = {
  hasField: 'E',
  hasType: 'E',
  childOf: 'E',
  dependsOn: 'E',
  reference: 'E',
  hasValue: 'E',
  type: 'V',
  field: 'V',
  instance: 'V',
  value: 'V',
}

const MAX_DELETE_PER_QUERY = 100000
const MAX_CREATE_PER_QUERY = 20000

type VertexDef = {
  type: string
  props: {
    id: string
    [attr: string]: unknown
  }
}

const vertexType = (elem: Element): string => {
  if (isType(elem)) return 'type'
  if (isInstanceElement(elem)) return `instance:${elem.type.elemID.name}`
  return 'field'
}

const elemToVertex = (elem: Element, withValues = true): VertexDef => ({
  type: vertexType(elem),
  props: {
    id: elem.elemID.getFullName(),
    name: elem.elemID.getFullNameParts().pop(),
    adapter: elem.elemID.adapter,
    ...withValues ? elem.annotations : {},
    ...withValues && isInstanceElement(elem) ? elem.value : {},
    ...isPrimitiveType(elem) ? { primitive: elem.primitive } : {},
  },
})

type EdgeDef = {
  type: keyof typeof DbClasses
  from: string
  to: string
  props: Record<string, unknown>
}
type EdgeProvider = (elem: Element) => EdgeDef[]

const fieldEdges: EdgeProvider = elem => (
  isObjectType(elem)
    ? Object.values(elem.fields).map(field => ({
      type: 'hasField',
      from: elem.elemID.getFullName(),
      to: field.elemID.getFullName(),
      props: { name: field.name },
    }))
    : []
)

const hasType = (elem: Element): elem is Field | InstanceElement => (
  isField(elem) || isInstanceElement(elem)
)

const typeEdges: EdgeProvider = elem => (
  hasType(elem)
    ? [{
      type: 'hasType',
      from: elem.elemID.getFullName(),
      to: elem.type.elemID.getFullName(),
      props: {},
    }]
    : []
)

const parentEdges: EdgeProvider = elem => (
  collections.array.makeArray(elem.annotations[INSTANCE_ANNOTATIONS.PARENT])
    .map((parent: ReferenceExpression) => ({
      type: 'childOf',
      from: elem.elemID.getFullName(),
      to: parent.elemId.getFullName(),
      props: {},
    }))
)

const dependsOnEdges: EdgeProvider = elem => (
  collections.array.makeArray(elem.annotations[INSTANCE_ANNOTATIONS.DEPENDS_ON])
    .map((parent: ReferenceExpression) => ({
      type: 'dependsOn',
      from: elem.elemID.getFullName(),
      to: parent.elemId.getFullName(),
      props: {},
    }))
)

const referenceEdges: (fromParent: boolean) => EdgeProvider = fromParent => elem => {
  if (isListType(elem)) return []
  const edges: EdgeDef[] = []
  const getAllRefs: TransformFunc = ({ value, path }) => {
    if (isReferenceExpression(value)) {
      edges.push({
        from: fromParent ? elem.elemID.getFullName() : path?.getFullName() as string,
        // TODO: there is a bug here in orientDB if only top level elements are in the graph
        to: value.elemId.getFullName(),
        type: 'reference',
        props: path === undefined ? {} : { src: path.getFullName() },
      })
    }
    return value
  }
  transformElement({
    element: elem,
    transformFunc: getAllRefs,
    strict: false,
  })
  return edges
}

const valueEdges: EdgeProvider = elem => {
  if (isType(elem)) return []
  const edges: EdgeDef[] = []
  const getValueEdges: TransformFunc = ({ value, path }) => {
    if (path !== undefined && path !== elem.elemID) {
      edges.push({
        from: path.createParentID().getFullName(),
        to: path.getFullName(),
        type: 'hasValue',
        props: { name: path.name },
      })
    }
    return value
  }
  transformElement({
    element: elem,
    transformFunc: getValueEdges,
    strict: false,
  })
  return edges
}

const listTypeEdges: EdgeProvider = elem => (
  isListType(elem)
    ? [{ from: elem.elemID.getFullName(), to: elem.innerType.elemID.getFullName(), type: 'hasType', props: {} }]
    : []
)

const elemToEdges = (providers: EdgeProvider[]): EdgeProvider => elem => (
  _.flatten(providers.map(provider => provider(elem)))
)

const neoDbClient = (url: string): DBClient => {
  const db = neo4j.driver(url, neo4j.auth.basic('neo4j', 'salto'))
  const session = db.session()
  return {
    clear: async () => {
      const clearData = async (): Promise<void> => {
        const tx = session.beginTransaction()
        let deleted = 0
        try {
          const res = await tx.run(`MATCH (n) WITH n LIMIT ${MAX_DELETE_PER_QUERY} DETACH DELETE n`)
          deleted = res.summary.counters.updates().nodesDeleted
          await tx.commit()
        } catch (e) {
          await tx.rollback()
          throw e
        }
        console.log('removed %d nodes', deleted)
        if (deleted === MAX_DELETE_PER_QUERY) {
          console.log('clearing more data')
          await clearData()
        }
      }
      const dropIndexes = async (): Promise<void> => {
        const tx = session.beginTransaction()
        try {
          const currentIndexes = await tx.run('CALL db.indexes')
          console.log('removing %d indexes', currentIndexes.records.length)
          await promises.array.series(
            currentIndexes.records.map(
              idx => (): Promise<QueryResult> => {
                console.log(`DROP INDEX ${idx.get('name')}`)
                return tx.run(`DROP INDEX ${idx.get('name')}`)
              }
            )
          )
          await tx.commit()
        } catch (e) {
          await tx.rollback()
          throw e
        }
      }

      await dropIndexes()
      await clearData()
    },
    createTypes: async elements => {
      const getVertices = (elem: Element): VertexDef[] => {
        const mainVertex = elemToVertex(elem, false)
        if (isType(elem)) return [mainVertex]
        const valueVertices: VertexDef[] = []
        const createValueNodes: TransformFunc = ({ value, path }) => {
          if (path !== undefined) {
            valueVertices.push({
              type: 'value',
              props: {
                id: path.getFullName(),
                name: path.getFullNameParts().pop(),
                ...isPrimitiveValue(value) ? { value } : {},
                // Hack to get restrictions to show up since they do not have a proper type
                ...path.name === '_restriction' ? value : {},
              },
            })
          }
          return value
        }
        transformElement({
          element: elem,
          transformFunc: createValueNodes,
          strict: false,
        })
        return [mainVertex, ...valueVertices]
      }
      const allElements = _.flatten(
        elements.map(elem => [elem, ...isObjectType(elem) ? Object.values(elem.fields) : []])
      )
      const vertexGroups = _(allElements)
        .map(getVertices)
        .flatten()
        .groupBy(v => v.type)
        .entries()
        .map(([type, nodes]) => _.chunk(nodes, MAX_CREATE_PER_QUERY).map(chunk => [type, chunk]))
        .flatten()

      const innerCreateNodes = async (type: string, nodes: VertexDef[]): Promise<void> => {
        const tx = session.beginTransaction()
        try {
          console.log('%d: creating %d nodes of type %s', Date.now(), nodes.length, type)
          await tx.run(`UNWIND $nodes AS v CREATE (n:${type}) SET n=v.props`, { nodes })
          await tx.commit()
        } catch (e) {
          await tx.rollback()
          // Binary search for the node that caused the error
          if (nodes.length > 1) {
            await innerCreateNodes(type, nodes.slice(0, nodes.length / 2))
            await innerCreateNodes(type, nodes.slice(nodes.length / 2))
          }
          console.log({ msg: 'Failed to create vertex', node: nodes[0] })
          throw e
        }
      }

      const createNodes = async (): Promise<void> => {
        await promises.array.series(
          vertexGroups
            .map(([type, nodes]) => async () => {
              await innerCreateNodes(type as string, nodes as VertexDef[])
            })
            .value()
        )
      }

      const createIdx = async (): Promise<void> => {
        const tx = session.beginTransaction()
        try {
          await promises.array.series(
            _(DbClasses)
              .pickBy(v => v === 'V')
              .keys()
              .map(type => () => Promise.all([
                tx.run(`CREATE INDEX ${type}_id_idx FOR (n:${type}) ON (n.id)`),
                tx.run(`CREATE INDEX ${type}_name_idx FOR (n:${type}) ON (n.name)`),
              ]))
              .value()
          )
          await tx.commit()
        } catch (e) {
          await tx.rollback()
          throw e
        }
      }

      await createNodes()
      await createIdx()
    },
    createRelationships: async elements => {
      const nodeType = (idStr: string): string => {
        const id = ElemID.fromFullName(idStr)
        if (id.isTopLevel()) {
          return id.idType
        }
        if (id.idType === 'field' && id.nestingLevel === 1) {
          return 'field'
        }
        return 'value'
      }

      const allElements = _.flatten(
        elements.map(elem => [elem, ...isObjectType(elem) ? Object.values(elem.fields) : []])
      )
      const edgeProvider = elemToEdges([
        fieldEdges, typeEdges, parentEdges, dependsOnEdges, // referenceEdges(true),
        referenceEdges(false), valueEdges, listTypeEdges,
      ])

      const allEdges = _(allElements)
        .map(edgeProvider)
        .flatten()
        .groupBy(edge => [edge.type, nodeType(edge.from), nodeType(edge.to)].join('___'))

      await promises.array.series(
        allEdges
          .entries()
          .map(([edgeGroupKey, edges]) => () => {
            const [type, fromType, toType] = edgeGroupKey.split('___')
            console.log('%d, creating %d edges of type %s (from %s to %s)', Date.now(), edges.length, type, fromType, toType)
            return promises.array.series(
              _.chunk(edges, MAX_CREATE_PER_QUERY).map(edgeChunk => async () => {
                const tx = session.beginTransaction()
                try {
                  await tx.run(
                    `UNWIND $edges AS edge MATCH (s:${fromType}), (d:${toType}) WHERE s.id=edge.from AND d.id=edge.to CREATE (s)-[e:${type}]->(d) SET e=edge.props`,
                    { edges: edgeChunk },
                  )
                  await tx.commit()
                } catch (e) {
                  await tx.rollback()
                  throw e
                }
              })
            )
          })
          .value()
      )
    },
  }
}

const main = async (): Promise<number> => {
  const args = yargs
    .string('workspace')
    .demand('workspace')
    .describe('workspace', 'The workspace to export')
    .string('db')
    .describe('db', 'The database to fill')
    .default('db', 'localhost')
    .help()
    .argv

  console.log('Loading workspace...')
  const ws = await loadLocalWorkspace(args.workspace as string)
  const elements = await ws.elements()
  await ws.flush()

  try {
    const client = await neoDbClient(`bolt://${args.db}`)
    const allTypes = [...elements, ...Object.values(BuiltinTypes)]
    console.log('clearing DB')
    await client.clear()
    console.log('creating type nodes')
    await client.createTypes(allTypes)
    console.log('creating relationships')
    await client.createRelationships(allTypes)
  } catch (e) {
    console.log('Failed transaction with error')
    console.log(e)
  }

  return 0
}

main().then(exitCode => process.exit(exitCode))
