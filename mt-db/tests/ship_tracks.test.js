import { test, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { client } from "./db";

beforeAll(async () => {
  await client.connect();
});

afterAll(async () => {
  await client.end();
});

beforeEach(async () => {
  await client.query(`
    TRUNCATE ship_positions, ship_tracks, ships RESTART IDENTITY CASCADE;
  `);
});

describe("Table: ship_tracks", () => {
  test("creates ship_track and links position with matching ship_id", async () => {
    const {
      rows: [ship],
    } = await client.query(`
    INSERT INTO ships (mt_id, ship_type, gt_ship_type, flag)
    VALUES ('mt-track', 70, 100, 'RU') RETURNING *;
  `);

    const {
      rows: [track],
    } = await client.query(
      `
    INSERT INTO ship_tracks (ship_id) VALUES ($1) RETURNING *;
  `,
      [ship.id]
    );

    expect(track.ship_id).toBe(ship.id);

    const {
      rows: [position],
    } = await client.query(
      `
    INSERT INTO ship_positions (
      ship_id, track_id, position, speed, course, heading, rot,
      elapsed, tile_x, tile_y, tile_z, reported_at
    )
    VALUES (
      $1, $2, ST_SetSRID(ST_Point(10, 10), 4326),
      12, 100, 100, 0, 0, 1, 1, 1, NOW()
    )
    RETURNING *;
  `,
      [ship.id, track.id]
    );

    expect(position.track_id).toBe(track.id);
  });

  test("rejects ship_position if track_id does not match ship_id (trigger)", async () => {
    const ship1 = await client.query(`
    INSERT INTO ships (mt_id, ship_type, gt_ship_type, flag)
    VALUES ('mt-s1', 1, 1, 'RU') RETURNING *;
  `);
    const ship2 = await client.query(`
    INSERT INTO ships (mt_id, ship_type, gt_ship_type, flag)
    VALUES ('mt-s2', 2, 2, 'US') RETURNING *;
  `);
    const track = await client.query(
      `
    INSERT INTO ship_tracks (ship_id) VALUES ($1) RETURNING *;
  `,
      [ship1.rows[0].id]
    );

    await expect(
      client.query(
        `
    INSERT INTO ship_positions (
      ship_id, track_id, position, speed, course, heading, rot,
      elapsed, tile_x, tile_y, tile_z, reported_at
    )
    VALUES (
      $1, $2, ST_SetSRID(ST_Point(0, 0), 4326),
      5, 0, 0, 0, 0, 0, 0, 0, NOW()
    )
  `,
        [ship2.rows[0].id, track.rows[0].id]
      )
    ).rejects.toThrow(/Track .* does not belong to ship/);
  });

  test("allows setting track_id to NULL again", async () => {
    const {
      rows: [ship],
    } = await client.query(`
    INSERT INTO ships (mt_id, ship_type, gt_ship_type, flag)
    VALUES ('mt-null', 1, 1, 'CN') RETURNING *;
  `);

    const {
      rows: [track],
    } = await client.query(
      `
    INSERT INTO ship_tracks (ship_id) VALUES ($1) RETURNING *;
  `,
      [ship.id]
    );

    const {
      rows: [position],
    } = await client.query(
      `
    INSERT INTO ship_positions (
      ship_id, track_id, position, speed, course, heading, rot,
      elapsed, tile_x, tile_y, tile_z, reported_at
    )
    VALUES (
      $1, $2, ST_SetSRID(ST_Point(0, 0), 4326),
      5, 0, 0, 0, 0, 0, 0, 0, NOW()
    )
    RETURNING *;
  `,
      [ship.id, track.id]
    );

    expect(position.track_id).toBe(track.id);

    const result = await client.query(
      `
    UPDATE ship_positions
    SET track_id = NULL
    WHERE id = $1 RETURNING *;
  `,
      [position.id]
    );

    expect(result.rows[0].track_id).toBeNull();
  });

  test("uses index on ship_tracks.ship_id", async () => {
    const explain = await client.query(`
    EXPLAIN SELECT * FROM ship_tracks WHERE ship_id = 123;
  `);
    const plan = explain.rows.map((r) => r["QUERY PLAN"]).join("\n");
    expect(plan).toMatch(/Index Scan|Bitmap Index Scan|Index Cond/);
  });
});
