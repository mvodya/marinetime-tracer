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
  pgm.createTable("ship_tracks", {
    id: { type: "bigserial", primaryKey: true },
    ship_id: {
      type: "bigint",
      notNull: true,
      references: "ships",
      onDelete: "CASCADE",
    },
    created_at: { type: "timestamptz", default: pgm.func("now()") },
  });

  // Add relation ship_positions -> ship_tracks
  pgm.addColumn("ship_positions", {
    track_id: {
      type: "bigint",
      notNull: false,
      references: "ship_tracks",
      onDelete: "SET NULL",
      default: null,
    },
  });
  pgm.createIndex("ship_positions", "track_id");

  // Create index for fast search by ship
  pgm.createIndex("ship_tracks", "ship_id");

  // Add trigger for checking track_id in ship_positions
  pgm.createFunction(
    "check_track_ship_consistency",
    [],
    {
      returns: "trigger",
      language: "plpgsql",
    },
    `
    BEGIN
       IF NEW.track_id IS NOT NULL THEN
         IF (SELECT ship_id FROM ship_tracks WHERE id = NEW.track_id) <> NEW.ship_id THEN
           RAISE EXCEPTION 'Track % does not belong to ship %', NEW.track_id, NEW.ship_id;
         END IF;
       END IF;
       RETURN NEW;
    END;
  `
  );
  pgm.createTrigger("ship_positions", "check_track_ship", {
    when: "BEFORE",
    operation: ["INSERT", "UPDATE OF track_id"],
    function: "check_track_ship_consistency",
    level: "ROW",
  });
};

/**
 * @param pgm {import('node-pg-migrate').MigrationBuilder}
 * @param run {() => void | undefined}
 * @returns {Promise<void> | void}
 */
export const down = (pgm) => {
  pgm.dropTrigger("ship_positions", "check_track_ship");
  pgm.dropFunction("check_track_ship_consistency");
  pgm.dropIndex("ship_tracks", "ship_id");
  pgm.dropIndex("ship_positions", "track_id");
  pgm.dropColumn("ship_positions", "track_id");
  pgm.dropTable("ship_tracks");
};
