/**
 * @type {import('node-pg-migrate').ColumnDefinitions | undefined}
 */
export const shorthands = undefined;

/**
 * @param pgm {import('node-pg-migrate').MigrationBuilder}
 * @param run {() => void | undefined}
 * @returns {Promise<void> | void}
 */
export const up = (pgm) => {
  // Ships with characteristics table
  pgm.createTable("ships", {
    id: { type: "bigserial", primaryKey: true }, // Local auto-incrementing primary key
    mt_id: { type: "text", unique: true }, // Marinetraffic identifier
    name: { type: "text", notNull: false }, // Ship name
    flag: { type: "text", notNull: false }, // Flag of the country of registration (e.g. RU, CN)
    ship_type: { type: "integer", notNull: true }, // Raw AIS ship type code
    gt_ship_type: { type: "integer", notNull: true }, // Normalized/classified ship type
    length: { type: "integer", notNull: false, check: "length >= 0" }, // Ship length in meters
    width: { type: "integer", notNull: false, check: "width >= 0" }, // Ship width in meters
    dwt: { type: "integer", notNull: false, check: "dwt >= 0" }, // Deadweight tonnage of the ship
    created_at: { type: "timestamptz", default: pgm.func("now()") }, // Timestamp when this record was created
  });

  // For analytics
  pgm.createIndex("ships", ["gt_ship_type", "ship_type"]);
  pgm.createIndex("ships", "flag");
};

/**
 * @param pgm {import('node-pg-migrate').MigrationBuilder}
 * @param run {() => void | undefined}
 * @returns {Promise<void> | void}
 */
export const down = (pgm) => {
  pgm.dropIndex("ships", "flag");
  pgm.dropIndex("ships", ["gt_ship_type", "ship_type"]);
  pgm.dropTable("ships");
};
