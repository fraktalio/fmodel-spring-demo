-- #######################################################################################
-- ######                                EVENT STREAMING                            ######
-- #######################################################################################


-- The views table is a registry of all views/subscribers that are able to subscribe to all events with a "pooling_delay" frequency.
-- You can not start consuming events without previously registering the view.
-- see: `stream_events` function
CREATE TABLE views
(
    -- view identifier/name
    "view"          TEXT,
    -- pooling_delay represent the frequency of pooling the database for the new events / 500 ms by default
    "pooling_delay" BIGINT                   DEFAULT 500   NOT NULL,
    -- the point in time form where the event streaming/pooling should start / NOW is by default, but you can specify the binging of time if you want
    "start_at"      TIMESTAMP                DEFAULT NOW() NOT NULL,
    -- the timestamp of the view insertion.
    "created_at"    TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    -- the timestamp of the view update.
    "updated_at"    TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    PRIMARY KEY ("view")
);

-- The locks table is designed to allow multiple concurrent, uncoordinated consumers to safely read/stream events per view
-- It can be only one transaction consuming events for the same decider_id/partition, but many transactions can concurrently consume events belonging to different decider_id's, without contention.
-- see: `stream_events` function
CREATE TABLE IF NOT EXISTS locks
(
    -- view identifier/name
    "view"         TEXT                                                    NOT NULL,
    -- business identifier for the decider
    "decider_id"   TEXT                                                    NOT NULL,
    -- current offset of the event stream for decider_id
    "offset"       BIGINT                                                  NOT NULL,
    -- the offset of the last event being processed
    "last_offset"  BIGINT                                                  NOT NULL,
    -- a lock / is this event stream for particular decider_id locked for reading or not
    "locked_until" TIMESTAMP WITH TIME ZONE DEFAULT NOW() - INTERVAL '1ms' NOT NULL,
    -- an indicator if the offset is final / offset will not grow any more
    "offset_final" BOOLEAN                                                 NOT NULL,
    -- the timestamp of the view insertion.
    "created_at"   TIMESTAMP WITH TIME ZONE DEFAULT NOW()                  NOT NULL,
    -- the timestamp of the view update.
    "updated_at"   TIMESTAMP WITH TIME ZONE DEFAULT NOW()                  NOT NULL,
    PRIMARY KEY ("view", "decider_id"),
    FOREIGN KEY ("view") REFERENCES views ("view") ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS locks_index ON locks ("decider_id", "locked_until", "last_offset");

-- SIDE EFFECT:  before_update_views_table - automatically bump "updated_at" when modifying a view
CREATE OR REPLACE FUNCTION "before_update_views_table"() RETURNS trigger AS
'
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
'
    LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS "t_before_update_views_table" ON "views";
CREATE TRIGGER "t_before_update_views_table"
    BEFORE UPDATE
    ON "views"
    FOR EACH ROW
EXECUTE FUNCTION "before_update_views_table"();

-- SIDE EFFECT:  before_update_locks_table - automatically bump "updated_at" when modifying a lock
CREATE OR REPLACE FUNCTION "before_update_locks_table"() RETURNS trigger AS
'
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
'
    LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS "t_before_update_locks_table" ON "locks";
CREATE TRIGGER "t_before_update_locks_table"
    BEFORE UPDATE
    ON "locks"
    FOR EACH ROW
EXECUTE FUNCTION "before_update_locks_table"();

--  SIDE EFFECT: after appending a new event (with new decider_id), the lock is upserted
CREATE OR REPLACE FUNCTION on_insert_on_events() RETURNS trigger AS
'
    BEGIN

        INSERT INTO locks
        SELECT t1.view        AS view,
               NEW.decider_id AS decider_id,
               NEW.offset     AS offset,
               0              AS last_offset,
               NOW()          AS locked_until,
               NEW.final      AS offset_final
        FROM views AS t1
        ON CONFLICT ON CONSTRAINT "locks_pkey" DO UPDATE SET "offset"     = NEW."offset",
                                                             offset_final = NEW.final;
        RETURN NEW;
    END;
'
    LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS t_on_insert_on_events ON events;
CREATE TRIGGER t_on_insert_on_events
    AFTER INSERT
    ON events
    FOR EACH ROW
EXECUTE FUNCTION on_insert_on_events();



-- SIDE EFFECT: after upserting a views, all the locks should be re-upserted so to keep the correct matrix of `view-deciderId` locks
CREATE OR REPLACE FUNCTION on_insert_or_update_on_views() RETURNS trigger AS
'
    BEGIN
        INSERT INTO locks
        SELECT NEW."view"    AS "view",
               t1.decider_id AS decider_id,
               t1.max_offset AS "offset",
               COALESCE(
                       (SELECT t2."offset" - 1 AS "offset"
                        FROM events AS t2
                        WHERE t2.decider_id = t1.decider_id
                          AND t2.created_at >= NEW.start_at
                        ORDER BY t2."offset" ASC
                        LIMIT 1),
                       (SELECT t2."offset" AS "offset"
                        FROM events AS t2
                        WHERE t2.decider_id = t1.decider_id
                        ORDER BY "t2"."offset" DESC
                        LIMIT 1)
                   )         AS last_offset,
               NOW()         AS locked_until,
               t1.final      AS offset_final
        FROM (SELECT DISTINCT ON (decider_id) decider_id AS decider_id,
                                              "offset"   AS max_offset,
                                              final      AS final
              FROM events
              ORDER BY decider_id, "offset" DESC) AS t1
        ON CONFLICT ON CONSTRAINT "locks_pkey"
            DO UPDATE
            SET "offset"     = EXCLUDED."offset",
                last_offset  = EXCLUDED.last_offset,
                offset_final = EXCLUDED.offset_final;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS t_on_insert_or_update_on_views ON "views";
CREATE TRIGGER t_on_insert_or_update_on_views
    AFTER INSERT OR UPDATE
    ON "views"
    FOR EACH ROW
EXECUTE FUNCTION on_insert_or_update_on_views();


-- ############################################ API ############################################


-- Register a `view` (responsible for streaming events to concurrent consumers)
-- Once the `view` is registered you can start `read_events` which will stream events by pooling database with delay, filtering `events` that are created after `start_at` timestamp
-- Example of usage: SELECT * from register_view('view1', 500, '2023-01-23 12:17:17.078384')
CREATE OR REPLACE FUNCTION register_view(v_view TEXT, v_pooling_delay BIGINT, v_start_at TIMESTAMP)
    RETURNS SETOF "views" AS
'
    BEGIN
        RETURN QUERY
            INSERT INTO "views" ("view", pooling_delay, start_at)
                VALUES (v_view, v_pooling_delay, v_start_at) RETURNING *;
    END;
' LANGUAGE plpgsql;

-- Get event(s) for the view - event streaming to concurrent consumers in a safe way
-- Concurrent Views/Subscribers can not stream/read events from one decider_id stream (partition) at the same time, because `lock` is preventing it.
-- They can read events concurrently from different decider_id streams (partitions) by preserving the ordering of events within decider_id stream (partition) only!
-- Example of usage: SELECT * from stream_events('view1', 1)
-- CREATE OR REPLACE FUNCTION stream_events(v_view_name TEXT, v_limit INTEGER)
CREATE OR REPLACE FUNCTION stream_events(v_view_name TEXT)
    RETURNS SETOF events AS
'
    DECLARE
        v_last_offset INTEGER;
        v_decider_id  TEXT;
    BEGIN
        -- Check if there are events with a greater id than the last_offset and acquire lock on views table/row for the first decider_id/stream you can find
        SELECT decider_id,
               last_offset
        INTO v_decider_id, v_last_offset
        FROM locks
        WHERE view = v_view_name
          AND locked_until < NOW() -- locked = false
          AND last_offset < "offset"
        ORDER BY "offset"
        LIMIT 1 FOR UPDATE SKIP LOCKED;

        -- Update views locked status to true
        UPDATE locks
        SET locked_until = NOW() + INTERVAL ''5m'' -- locked = true, for next 5 minutes
        WHERE view = v_view_name
          AND locked_until < NOW() -- locked = false
          AND decider_id = v_decider_id;

        -- Return the events that have not been locked yet
        RETURN QUERY SELECT *
                     FROM events
                     WHERE decider_id = v_decider_id
                       AND "offset" > v_last_offset
                     ORDER BY "offset"
                     LIMIT 1;
    END;
' LANGUAGE plpgsql;

-- Acknowledge that event with `decider_id` and `offset` is processed by the view
-- Essentially, it will unlock current decider_id stream (partition) for further reading
-- Example of usage: SELECT * from ack_event('view1', 'f156a3c4-9bd8-11ed-a8fc-0242ac120002', 1)
CREATE OR REPLACE FUNCTION ack_event(v_view TEXT, v_decider_id TEXT, v_offset BIGINT)
    RETURNS SETOF "locks" AS
'
    BEGIN
        -- Update locked status to false
        RETURN QUERY
            UPDATE locks
                SET locked_until = NOW(), -- locked = false,
                    last_offset = v_offset
                WHERE "view" = v_view
                    AND decider_id = v_decider_id
                RETURNING *;
    END;
' LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nack_event(v_view TEXT, v_decider_id TEXT)
    RETURNS SETOF "locks" AS
'
    BEGIN
        -- Nack: Update locked status to false, without updating the offset
        RETURN QUERY
            UPDATE locks
                SET locked_until = NOW() -- locked = false
                WHERE "view" = v_view
                    AND decider_id = v_decider_id
                RETURNING *;
    END;
' LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION schedule_nack_event(v_view TEXT, v_decider_id TEXT, v_milliseconds BIGINT)
    RETURNS SETOF "locks" AS
'
    BEGIN
        -- Schedule the nack
        RETURN QUERY
            UPDATE locks
                SET "locked_until" = NOW() + (v_milliseconds || ''ms'')::INTERVAL
                WHERE "view" = v_view
                    AND decider_id = v_decider_id
                RETURNING *;
    END;
' LANGUAGE plpgsql;
