import { Operon } from 'src/';
import { Pool } from 'pg';
import fs from 'fs';

jest.mock('fs');

describe('Operon config', () => {
    test('fs.stat fails', () => {
        jest.spyOn(fs, 'stat').mockImplementation((path, callback) => {
            throw new Error('An error');
        });
        expect(() => new Operon()).toThrow('calling fs.stat on operon-config.yaml: An error');
    });

    test.only('System error while checking on config file', () => {
    });

    test('Config file is not a valid file', () => {
    });

    test('Config is valid and is parsed as expected', () => {
        const mockDatabaseConfig = {
            hostname: 'some host',
            port: 1111,
            username: 'some test user',
            password: 'some test password',
            connectionTimeoutMillis: 3,
        };
        const mockConfigFile = {
            database: mockDatabaseConfig,
        };

        jest.spyOn(fs, 'readFileSync').mockReturnValueOnce(JSON.stringify(mockConfigFile));
        const operon = new Operon();
        const pool: Pool = operon.pool as Pool;
        // Pool is subclassed as BoundPool by pg-node.
        // But pg only exports the `pool` type
        // So we do some terrible things to retrieve the `options` property
        expect(pool.hasOwnProperty('options')).toBe(true);
        const options = Object.getOwnPropertyDescriptors(pool)['options'].value;
        expect(options.host).toBe(mockDatabaseConfig.hostname);
        expect(options.port).toBe(mockDatabaseConfig.port);
        expect(options.user).toBe(mockDatabaseConfig.username);
        expect(options.password).toBe(mockDatabaseConfig.password);
        expect(options.connectionTimeoutMillis).toBe(mockDatabaseConfig.connectionTimeoutMillis);
        expect(options.database).toBe('postgres');
        expect(options.max).toBe(10);
    });
});
