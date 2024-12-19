const { createCargo, isInited, init } = require('../');
const assert = require('assert');

const QUERY = 'test.cargo0';
const QUERY1 = 'test.cargo1';

describe('init clickhouse_cargo', () => {
  let cargo0 = null;

  after(() => {
    process.exit(0);
  });

  it('auto init when env set', (done) => {
    assert(isInited(), 'should auto init when env set');
    done();
  });

  it('cargo must be created with tableName', (done) => {
    assert.throws(() => {
      createCargo();
    }, Error, /tableName/);

    assert.throws(() => {
      createCargo('select * from dual');
    }, Error, /tableName/);

    cargo0 = createCargo(QUERY);
    //console.log(cargo0);
    assert(cargo0);
    assert(cargo0.tableName === QUERY, 'bad cargo0.tableName');
    //assert(cargo0.maxTime === INIT_OPTION.maxTime, `bad cargo0.maxTime:${cargo0.maxTime} => ${INIT_OPTION.maxTime}`);
    //assert(cargo0.maxRows === INIT_OPTION.maxRows, 'bad cargo0.maxRows');
    //assert(cargo0.commitInterval === INIT_OPTION.commitInterval, 'bad cargo0.commitInterval');
    done();
  });

  it('create multiple cargoes with the same statement should result in one shared cargo', (done) => {
    const cargo1 = createCargo(QUERY);
    const cargo2 = createCargo(QUERY);
    const cargo3 = createCargo(QUERY);
    assert(cargo0 === cargo1 && cargo1 === cargo2 && cargo2 === cargo3);
    const cargo4 = createCargo(QUERY1);
    assert(cargo4 !== cargo3);
    done();
  });
});
