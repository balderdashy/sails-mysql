/**
 * Fixture Schema To Pass To Define
 */

module.exports = {
  first_name: { type: 'string' },
  last_name: { type: 'string' },
  email: { type: 'string' },
  id:{
    type: 'integer',
    autoIncrement: true,
    defaultsTo: 'AUTO_INCREMENT',
    primaryKey: true
  },
  createdAt: { type: 'DATE', default: 'NOW' },
  updatedAt: { type: 'DATE', default: 'NOW' }
};
