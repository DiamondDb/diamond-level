const DiamondLevel = require('./diamondLevel')
const DiamondDB = require('DiamondDB')

const store = new DiamondLevel()

/* create a database instance with our cache and store modules */
const db = new DiamondDB.Database({
  store,
  cache: false
})

/* initialize the db and configure it to write to disk every five seconds */
/* set persist to false to turn off intermittent batch persistence */
db.init({
  persist: 5000
})

DiamondDB.server(db)
