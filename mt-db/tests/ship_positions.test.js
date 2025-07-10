import {
  beforeAll,
  afterAll,
  beforeEach,
  test,
  expect,
  describe,
} from "vitest";
import { client } from "./db";

beforeAll(async () => {
  await client.connect();
});

afterAll(async () => {
  await client.end();
});

beforeEach(async () => {
  await client.query("TRUNCATE ship_positions, ships RESTART IDENTITY CASCADE");
});

describe("Table: ship_positions", () => {
  let shipId;

  beforeEach(async () => {
    const {
      rows: [ship],
    } = await client.query(`
      INSERT INTO ships (mt_id, ship_type, gt_ship_type, flag)
      VALUES ('mt-geo-1', 60, 110, 'US')
      RETURNING id;
    `);
    shipId = ship.id;
  });

  test("inserts full valid position record", async () => {
    const {
      rows: [pos],
    } = await client.query(
      `
      INSERT INTO ship_positions (
        ship_id, position, speed, course, heading, rot,
        elapsed, destination, tile_x, tile_y, tile_z, reported_at
      )
      VALUES (
        $1, ST_SetSRID(ST_Point(45.0, 60.0), 4326), 10, 90, 90, 0,
        15, 'Tokyo', 1, 2, 3, NOW()
      )
      RETURNING *;
    `,
      [shipId]
    );

    expect(pos.id).toBeDefined();
    expect(pos.ship_id).toBe(shipId);
    expect(pos.speed).toBe(10);
    expect(pos.position).toBeDefined();
    expect(pos.destination).toBe("Tokyo");
  });

  test("rejects position without ship_id (FK constraint)", async () => {
    await expect(
      client.query(`
      INSERT INTO ship_positions (
        ship_id, position, speed, course, heading, rot,
        elapsed, tile_x, tile_y, tile_z, reported_at
      )
      VALUES (
        99999, ST_SetSRID(ST_Point(0,0), 4326), 5, 0, 0, 0,
        0, 0, 0, 0, now()
      )
    `)
    ).rejects.toThrow(/violates foreign key constraint/);
  });

  test("rejects negative values for speed/course/heading/elapsed", async () => {
    const base = `
      INSERT INTO ship_positions (
        ship_id, position, speed, course, heading, rot,
        elapsed, tile_x, tile_y, tile_z, reported_at
      )
      VALUES ($1, ST_SetSRID(ST_Point(10,10), 4326), $2, $3, $4, $5, $6, 0,0,0, now());
    `;

    const valuesList = [
      [-1, 0, 0, 0], // speed < 0
      [0, -1, 0, 0], // course < 0
      [0, 0, -1, 0], // heading < 0
      [0, 0, 0, -1], // elapsed < 0
    ];

    for (const [speed, course, heading, elapsed] of valuesList) {
      await expect(
        client.query(base, [shipId, speed, course, heading, 0, elapsed])
      ).rejects.toThrow(/violates check constraint/);
    }
  });

  test("ST_Contains works with position geometry", async () => {
    await client.query(
      `
      INSERT INTO ship_positions (
        ship_id, position, speed, course, heading, rot,
        elapsed, tile_x, tile_y, tile_z, reported_at
      )
      VALUES (
        $1, ST_SetSRID(ST_Point(50, 50), 4326), 5, 180, 180, 0,
        5, 1, 1, 1, NOW()
      );
    `,
      [shipId]
    );

    const { rowCount } = await client.query(`
      SELECT * FROM ship_positions
      WHERE ST_Contains(
        ST_MakeEnvelope(49.9, 49.9, 50.1, 50.1, 4326),
        position
      );
    `);

    expect(rowCount).toBe(1);
  });

  test("reported_at must be present", async () => {
    await expect(
      client.query(
        `
      INSERT INTO ship_positions (
        ship_id, position, speed, course, heading, rot,
        elapsed, tile_x, tile_y, tile_z
      )
      VALUES (
        $1, ST_SetSRID(ST_Point(10, 10), 4326), 0, 0, 0, 0,
        0, 1, 2, 3
      );
    `,
        [shipId]
      )
    ).rejects.toThrow(/null value in column "reported_at"/);
  });
});
