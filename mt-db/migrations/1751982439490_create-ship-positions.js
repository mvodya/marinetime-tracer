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
  pgm.createTable("ship_positions", {
    id: { type: "bigserial", primaryKey: true },
    ship_id: {
      type: "bigint",
      notNull: true,
      references: "ships",
      onDelete: "cascade",
    },
    position: { type: "geometry(Point, 4326)", notNull: true }, // Geographical point (WGS84)
    speed: { type: "integer", notNull: false, check: "speed >= 0" }, // Speed over ground
    course: { type: "integer", notNull: false, check: "course >= 0" }, // Course over ground
    heading: { type: "integer", notNull: false, check: "heading >= 0" }, // True heading
    rot: { type: "integer", notNull: false }, // Rate of turn
    elapsed: { type: "integer", notNull: false, check: "elapsed >= 0" }, // Elapsed time since last known fix
    destination: { type: "text", notNull: false }, // Destination port
    tile_x: { type: "integer", notNull: false }, // Tile X
    tile_y: { type: "integer", notNull: false }, // Tile Y
    tile_z: { type: "integer", notNull: false }, // Tile Z
    reported_at: { type: "timestamptz", notNull: true }, // Time of actual position report
  });

  // For time, ship & geo searching
  pgm.createIndex("ship_positions", ["ship_id", "reported_at"]);
  pgm.createIndex("ship_positions", "reported_at");
  pgm.createIndex("ship_positions", "position", { using: "gist" });
};

/**
 * @param pgm {import('node-pg-migrate').MigrationBuilder}
 * @param run {() => void | undefined}
 * @returns {Promise<void> | void}
 */
export const down = (pgm) => {
  pgm.dropIndex("ship_positions", "position", { using: "gist" });
  pgm.dropIndex("ship_positions", ["ship_id", "reported_at"]);
  pgm.dropIndex("ship_positions", "reported_at");
  pgm.dropTable("ship_positions");
};
