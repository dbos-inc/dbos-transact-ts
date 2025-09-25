import Koa from 'koa';
import Router from '@koa/router';
import cors from '@koa/cors';

import { DBOS } from '@dbos-inc/dbos-sdk';

import { DBOSKoa } from '../src';

import request from 'supertest';

const dhttp = new DBOSKoa();

describe('http-cors-tests', () => {
  let app: Koa;
  let appRouter: Router;

  beforeAll(async () => {
    DBOS.setConfig({ name: 'koa-cors', enableOTLP: true });
    return Promise.resolve();
  });

  beforeEach(async () => {
    await DBOS.launch();
    app = new Koa();
    appRouter = new Router();
    dhttp.registerWithApp(app, appRouter);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  // Check get with us as origin
  it('should allow requests without credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellod') // Default CORS policy
      .set('Origin', 'https://us.com');

    expect(response.headers['access-control-allow-origin']).toBe('https://us.com');
    expect(response.status).toBe(200);
  });
  it('should allow requests without credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellor') // Regular cors() defaults from Koa
      .set('Origin', 'https://us.com');

    expect(response.headers['access-control-allow-origin']).toBe('*');
    expect(response.status).toBe(200);
  });
  it('should allow requests without credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellos') // Custom whitelist cors() implementation
      .set('Origin', 'https://us.com');

    expect(response.headers['access-control-allow-origin']).toBe('https://us.com');
    expect(response.status).toBe(200);
  });

  // Check get with partner as origin
  it('should allow requests without credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellod') // Default CORS policy
      .set('Origin', 'https://partner.com');

    expect(response.headers['access-control-allow-origin']).toBe('https://partner.com');
    expect(response.status).toBe(200);
  });
  it('should allow requests without credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellor') // Regular cors() defaults from Koa
      .set('Origin', 'https://partner.com');

    expect(response.headers['access-control-allow-origin']).toBe('*');
    expect(response.status).toBe(200);
  });
  it('should allow requests without credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellos') // Custom whitelist cors() implementation
      .set('Origin', 'https://partner.com');

    expect(response.headers['access-control-allow-origin']).toBe('https://partner.com');
    expect(response.status).toBe(200);
  });

  // Check get with another origin
  it('should allow requests without credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellod') // Default CORS policy
      .set('Origin', 'https://crimeware.com');

    expect(response.headers['access-control-allow-origin']).toBe('https://crimeware.com');
    expect(response.status).toBe(200);
  });
  it('should allow requests without credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellor') // Regular cors() defaults from Koa
      .set('Origin', 'https://crimeware.com');

    expect(response.headers['access-control-allow-origin']).toBe('*');
    expect(response.status).toBe(200);
  });
  it('should allow requests without credentials from allowed origins - not this one', async () => {
    const response = await request(app.callback())
      .get('/hellos') // Custom whitelist cors() implementation
      .set('Origin', 'https://crimeware.com');

    expect(response.headers['access-control-allow-origin']).toBeUndefined();
    expect(response.status).toBe(200); // IRL this response will not be shared by the browser to the caller; POSTs could be preflighted.
  });

  // Check get with us as origin AND credentials
  it('should allow requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellod') // Default CORS policy
      .set('Origin', 'https://us.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.headers['access-control-allow-origin']).toBe('https://us.com');
    expect(response.headers['access-control-allow-credentials']).toBe('true');

    expect(response.status).toBe(200);
  });
  it('should allow requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellor') // Regular cors() defaults from Koa
      .set('Origin', 'https://us.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.headers['access-control-allow-origin']).toBe('*');
    expect(response.headers['access-control-allow-credentials']).toBeUndefined();
    // IRL the browser will hate this result of '*' and refuse to share response w/ client

    expect(response.status).toBe(200);
  });
  it('should allow requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellos') // Custom whitelist cors() implementation
      .set('Origin', 'https://us.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.headers['access-control-allow-origin']).toBe('https://us.com');
    expect(response.headers['access-control-allow-credentials']).toBe('true');

    expect(response.status).toBe(200);
  });

  // Check get with partner as origin AND credentials
  it('should allow requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellod') // Default CORS policy
      .set('Origin', 'https://partner.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.headers['access-control-allow-origin']).toBe('https://partner.com');
    expect(response.headers['access-control-allow-credentials']).toBe('true');

    expect(response.status).toBe(200);
  });
  it('should allow requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellor') // Regular cors() defaults from Koa
      .set('Origin', 'https://partner.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.headers['access-control-allow-origin']).toBe('*');
    expect(response.headers['access-control-allow-credentials']).toBeUndefined();
    // IRL the browser will hate this result of '*' and refuse to share response w/ client

    expect(response.status).toBe(200);
  });
  it('should allow requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellos') // Custom whitelist cors() implementation
      .set('Origin', 'https://partner.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.headers['access-control-allow-origin']).toBe('https://partner.com');
    expect(response.headers['access-control-allow-credentials']).toBe('true');

    expect(response.status).toBe(200);
  });

  it('should allow preflight requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .options('/hellos') // Custom whitelist cors() implementation
      .set('Access-Control-Request-Method', 'GET')
      .set('Origin', 'https://partner.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.status).toBe(204);
    expect(response.headers['access-control-allow-origin']).toBe('https://partner.com');
    expect(response.headers['access-control-allow-credentials']).toBe('true');
  });
  it('should allow preflight requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .options('/hellor') // Regular cors() defaults from Koa
      .set('Access-Control-Request-Method', 'GET')
      .set('Origin', 'https://crimewave.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.status).toBe(204);
    expect(response.headers['access-control-allow-origin']).toBe('*');
    expect(response.headers['access-control-allow-credentials']).toBeUndefined();
  });
  it('should allow preflight requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .options('/hellod') // Default CORS policy
      .set('Access-Control-Request-Method', 'GET')
      .set('Origin', 'https://crimewave.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.status).toBe(204);
    expect(response.headers['access-control-allow-origin']).toBe('https://crimewave.com');
    expect(response.headers['access-control-allow-credentials']).toBe('true');
    expect(response.headers['access-control-allow-headers']).toBe(
      'Origin,X-Requested-With,Content-Type,Accept,Authorization',
    );
  });

  // Check get with another origin AND credentials
  it('should allow requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellod') // Default CORS policy
      .set('Origin', 'https://crimewave.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.headers['access-control-allow-origin']).toBe('https://crimewave.com');
    expect(response.headers['access-control-allow-credentials']).toBe('true');

    expect(response.status).toBe(200);
  });
  it('should allow requests with credentials from allowed origins', async () => {
    const response = await request(app.callback())
      .get('/hellor') // Regular cors() defaults from Koa
      .set('Origin', 'https://crimewave.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.headers['access-control-allow-origin']).toBe('*');
    expect(response.headers['access-control-allow-credentials']).toBeUndefined();
    // IRL the browser will hate this result of '*' and refuse to share response w/ client

    expect(response.status).toBe(200);
  });
  it('should allow requests with credentials from allowed origins - not this one', async () => {
    const response = await request(app.callback())
      .get('/hellos') // Custom whitelist cors() implementation
      .set('Origin', 'https://crimewave.com')
      .set('Cookie', 'sessionId=abc123');

    expect(response.headers['access-control-allow-origin']).toBeUndefined();
    expect(response.headers['access-control-allow-credentials']).toBeUndefined();

    expect(response.status).toBe(200);
  });
});

export class TestEndpointsDefCORS {
  @dhttp.getApi('/hellod')
  static async hello() {
    return Promise.resolve({ message: 'hello!' });
  }
}

@dhttp.koaCors(cors())
export class TestEndpointsRegCORS {
  @dhttp.getApi('/hellor')
  static async hello() {
    return Promise.resolve({ message: 'hello!' });
  }
}

@dhttp.koaCors(
  cors({
    credentials: true,
    origin: (o: Koa.Context) => {
      const whitelist = ['https://us.com', 'https://partner.com'];
      const origin = o.request.header.origin ?? '*';
      if (whitelist && whitelist.length > 0) {
        return whitelist.includes(origin) ? origin : '';
      }
      return o.request.header.origin || '*';
    },
    allowMethods: 'GET,OPTIONS', // Need to have options for preflight.
  }),
)
export class TestEndpointsSpecCORS {
  @dhttp.getApi('/hellos')
  static async hello() {
    return Promise.resolve({ message: 'hello!' });
  }
}
