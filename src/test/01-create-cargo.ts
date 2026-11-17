import { createCargo, isInited } from '../';
import assert from 'assert';
import { supportsBadEscapeSetting } from '../cargo';
import type { Cargo } from '../cargo';

const QUERY = 'test.cargo0';
const QUERY1 = 'test.cargo1';

describe('init clickhouse_cargo', () => {
  let cargo0: Cargo | null = null;

  it('auto init when env set', (done) => {
    assert(isInited(), 'should auto init when env set');
    done();
  });

  it('cargo must be created with tableName', (done) => {
    assert.throws(() => {
      createCargo();
    }, /tableName/);

    assert.throws(() => {
      createCargo('select * from dual');
    }, /tableName/);

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

describe('supportsBadEscapeSetting', () => {
  // the setting was introduced in ClickHouse v24.4
  it('v24.4 is the first supported version', () => {
    assert.strictEqual(supportsBadEscapeSetting('24.4'), true);
    assert.strictEqual(supportsBadEscapeSetting('24.4.1'), true);
    assert.strictEqual(supportsBadEscapeSetting('24.4.2.2283'), true);
  });

  it('versions below v24.4 are unsupported', () => {
    assert.strictEqual(supportsBadEscapeSetting('24.3'), false);
    assert.strictEqual(supportsBadEscapeSetting('24.3.9.2913'), false);
    assert.strictEqual(supportsBadEscapeSetting('23.8'), false);
    assert.strictEqual(supportsBadEscapeSetting('1.1.54384'), false);
  });

  it('versions above v24 are supported', () => {
    assert.strictEqual(supportsBadEscapeSetting('25.1'), true);
    assert.strictEqual(supportsBadEscapeSetting('25.12.3.71'), true);
  });

  it('unparsable versions count as unsupported', () => {
    assert.strictEqual(supportsBadEscapeSetting(''), false);
    assert.strictEqual(supportsBadEscapeSetting('unknown'), false);
    assert.strictEqual(supportsBadEscapeSetting('24'), false); // no minor part
  });
});

// exit explicitly: the background exam loop keeps the process alive
after(() => {
  process.exit(0);
});
