const levelUp = require('level')
const utils = require('DiamondDB').utilities

const {
  UPDATE_META,
  STORE_RECORD,
  FETCH_RECORD,
  MAKE_TABLE,
  INITIALIZE_PERSISTANCE,
  PERSIST_ALL,
  success
} = utils.operations

const makeRecordKey = (table, id) => `__${table.name}_${id}`

module.exports = class DiamondLevel {
  constructor(location){
    this.db = levelUp(location || './data')
    this.tableKey = '__db_tables'
    this.put = utils.promisify(this.db.put.bind(this.db))
    this.get = utils.promisify(this.db.get.bind(this.db))
    this.batch = utils.promisify(this.db.batch.bind(this.db))

    this.mostRecentMeta = null
    this.recordMessages = []
  }
  clearMessages(){
    const messages = this.recordMessages.slice()
    this.recordMessages = []
    return messages
  }
  initialize(){
    return this.get(this.tableKey).then(tableString => {
      if(tableString.length){
        const tables = utils.schemaUtils.parseMeta(tableString)
        return success(tables)
      }
    })
  }
  updateMeta() {
    const tables = this.mostRecentMeta && this.mostRecentMeta.tables
    if(tables){
      let meta = ''
      Object.keys(tables).forEach(tableName => {
        meta += utils.schemaUtils.makeTableString(tables[tableName])
      })
      return this.put(this.tableKey, meta)
    } else {
      return Promise.resolve(success())
    }
  }
  makeTable({ tableData }){
    const tableString = utils.schemaUtils.makeTableString(tableData)
    return this.get(this.tableKey)
    .then(tables => {
      const updatedString = tables + tableString
      return this.put(this.tableKey, updatedString)
    }).catch((e) => {
        if(e.notFound){
          return this.put(this.tableKey, tableString)
        } else {
          return Promise.reject(e)
        }
    })
  }
  fetchRecord({ table, id}){
    const key = makeRecordKey(table, id)
    return this.get(key).then(record => {
      return success(utils.recordUtils.parseRecord(record, table.schema))
    })
  }
  persist(){
    const messages = this.clearMessages()
    const operationsArray = messages.map(msg => {
      const type = 'put'
      const key = makeRecordKey(msg.table, msg.id)
      const value = utils.recordUtils.makeRecordString(msg.table, msg.record)
      return { type, key, value }
    })
    if(this.mostRecentMeta && operationsArray.length){
      return this.updateMeta().then(() => {
        return this.batch(operationsArray)
      })
    } else {
      return this.batch(operationsArray)
    }
  }
  message(message){
    console.log('diamond level message: ', message.operation)
    switch(message.operation){
      case UPDATE_META:
        this.mostRecentMeta = message.data
        return Promise.resolve()
      case STORE_RECORD:
        this.recordMessages.push(message.data)
        return Promise.resolve()
      case FETCH_RECORD:
        return this.fetchRecord(message.data)
      case MAKE_TABLE:
        return this.makeTable(message.data)
      case INITIALIZE_PERSISTANCE:
        return this.initialize()
      case PERSIST_ALL:
        return this.persist()
    }
  }
}
