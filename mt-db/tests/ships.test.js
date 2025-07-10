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
  await client.query("TRUNCATE ships RESTART IDENTITY CASCADE");
});

describe("Table: ships", () => {
  test("inserts a full valid ship record", async () => {
    const {
      rows: [ship],
    } = await client.query(`
      INSERT INTO ships (mt_id, name, flag, ship_type, gt_ship_type, length, width, dwt)
      VALUES ('mt-full-1', 'Test Ship', 'RU', 70, 100, 200, 30, 40000)
      RETURNING *;
    `);

    expect(ship.id).toBeDefined();
    expect(ship.name).toBe("Test Ship");
    expect(ship.flag).toBe("RU");
    expect(ship.length).toBe(200);
    expect(new Date(ship.created_at).getTime()).toBeLessThan(Date.now());
  });

  test("rejects duplicate mt_id", async () => {
    await client.query(`
      INSERT INTO ships (mt_id, ship_type, gt_ship_type)
      VALUES ('mt-dup-1', 70, 100);
    `);

    await expect(
      client.query(`
      INSERT INTO ships (mt_id, ship_type, gt_ship_type)
      VALUES ('mt-dup-1', 80, 110);
    `)
    ).rejects.toThrow(/duplicate key value violates unique constraint/);
  });

  test("rejects null ship_type or gt_ship_type", async () => {
    await expect(
      client.query(`
      INSERT INTO ships (mt_id)
      VALUES ('mt-null-type');
    `)
    ).rejects.toThrow(/null value in column "ship_type"/);
  });

  test("rejects negative length/width/dwt", async () => {
    await expect(
      client.query(`
      INSERT INTO ships (mt_id, ship_type, gt_ship_type, length)
      VALUES ('mt-neg-length', 70, 100, -1);
    `)
    ).rejects.toThrow(/violates check constraint/);

    await expect(
      client.query(`
      INSERT INTO ships (mt_id, ship_type, gt_ship_type, width)
      VALUES ('mt-neg-width', 70, 100, -5);
    `)
    ).rejects.toThrow(/violates check constraint/);

    await expect(
      client.query(`
      INSERT INTO ships (mt_id, ship_type, gt_ship_type, dwt)
      VALUES ('mt-neg-dwt', 70, 100, -1000);
    `)
    ).rejects.toThrow(/violates check constraint/);
  });

  test("allows optional fields to be null", async () => {
    const {
      rows: [ship],
    } = await client.query(`
      INSERT INTO ships (mt_id, ship_type, gt_ship_type, flag)
      VALUES ('mt-partial', 99, 199, 'US')
      RETURNING *;
    `);

    expect(ship.name).toBeNull();
    expect(ship.length).toBeNull();
    expect(ship.dwt).toBeNull();
  });

  test("indexes exist and are usable (manual check)", async () => {
    const explain = await client.query(`
      EXPLAIN SELECT * FROM ships WHERE flag = 'RU';
    `);

    const planText = explain.rows.map((r) => r["QUERY PLAN"]).join("\n");
    expect(planText).toMatch(/Index Scan|Bitmap Index Scan/);
  });
});
