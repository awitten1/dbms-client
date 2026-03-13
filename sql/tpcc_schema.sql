-- TPC-C Schema
-- Foreign keys are omitted for bulk-load performance; the spec does not require them.
-- "order" is double-quoted everywhere because ORDER is a reserved SQL keyword.

DROP TABLE IF EXISTS order_line  CASCADE;
DROP TABLE IF EXISTS new_order   CASCADE;
DROP TABLE IF EXISTS "order"     CASCADE;
DROP TABLE IF EXISTS history     CASCADE;
DROP TABLE IF EXISTS customer    CASCADE;
DROP TABLE IF EXISTS stock       CASCADE;
DROP TABLE IF EXISTS district    CASCADE;
DROP TABLE IF EXISTS warehouse   CASCADE;
DROP TABLE IF EXISTS item        CASCADE;

CREATE TABLE warehouse (
    w_id        SMALLINT        NOT NULL,
    w_name      VARCHAR(10)     NOT NULL,
    w_street_1  VARCHAR(20)     NOT NULL,
    w_street_2  VARCHAR(20)     NOT NULL,
    w_city      VARCHAR(20)     NOT NULL,
    w_state     CHAR(2)         NOT NULL,
    w_zip       CHAR(9)         NOT NULL,
    w_tax       NUMERIC(4,4)    NOT NULL,
    w_ytd       NUMERIC(12,2)   NOT NULL,
    PRIMARY KEY (w_id)
);

CREATE TABLE district (
    d_id            SMALLINT        NOT NULL,
    d_w_id          SMALLINT        NOT NULL,
    d_name          VARCHAR(10)     NOT NULL,
    d_street_1      VARCHAR(20)     NOT NULL,
    d_street_2      VARCHAR(20)     NOT NULL,
    d_city          VARCHAR(20)     NOT NULL,
    d_state         CHAR(2)         NOT NULL,
    d_zip           CHAR(9)         NOT NULL,
    d_tax           NUMERIC(4,4)    NOT NULL,
    d_ytd           NUMERIC(12,2)   NOT NULL,
    d_next_o_id     INT             NOT NULL,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer (
    c_id            INT             NOT NULL,
    c_d_id          SMALLINT        NOT NULL,
    c_w_id          SMALLINT        NOT NULL,
    c_first         VARCHAR(16)     NOT NULL,
    c_middle        CHAR(2)         NOT NULL,
    c_last          VARCHAR(16)     NOT NULL,
    c_street_1      VARCHAR(20)     NOT NULL,
    c_street_2      VARCHAR(20)     NOT NULL,
    c_city          VARCHAR(20)     NOT NULL,
    c_state         CHAR(2)         NOT NULL,
    c_zip           CHAR(9)         NOT NULL,
    c_phone         CHAR(16)        NOT NULL,
    c_since         TIMESTAMP       NOT NULL,
    c_credit        CHAR(2)         NOT NULL,
    c_credit_lim    NUMERIC(12,2)   NOT NULL,
    c_discount      NUMERIC(4,4)    NOT NULL,
    c_balance       NUMERIC(12,2)   NOT NULL,
    c_ytd_payment   NUMERIC(12,2)   NOT NULL,
    c_payment_cnt   SMALLINT        NOT NULL,
    c_delivery_cnt  SMALLINT        NOT NULL,
    c_data          VARCHAR(500)    NOT NULL,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

-- history has no primary key per spec; rows are append-only
CREATE TABLE history (
    h_c_id      INT             NOT NULL,
    h_c_d_id    SMALLINT        NOT NULL,
    h_c_w_id    SMALLINT        NOT NULL,
    h_d_id      SMALLINT        NOT NULL,
    h_w_id      SMALLINT        NOT NULL,
    h_date      TIMESTAMP       NOT NULL,
    h_amount    NUMERIC(6,2)    NOT NULL,
    h_data      VARCHAR(24)     NOT NULL
);

CREATE TABLE item (
    i_id        INT             NOT NULL,
    i_im_id     INT             NOT NULL,
    i_name      VARCHAR(24)     NOT NULL,
    i_price     NUMERIC(5,2)    NOT NULL,
    i_data      VARCHAR(50)     NOT NULL,
    PRIMARY KEY (i_id)
);

CREATE TABLE stock (
    s_i_id          INT             NOT NULL,
    s_w_id          SMALLINT        NOT NULL,
    s_quantity      SMALLINT        NOT NULL,
    s_dist_01       CHAR(24)        NOT NULL,
    s_dist_02       CHAR(24)        NOT NULL,
    s_dist_03       CHAR(24)        NOT NULL,
    s_dist_04       CHAR(24)        NOT NULL,
    s_dist_05       CHAR(24)        NOT NULL,
    s_dist_06       CHAR(24)        NOT NULL,
    s_dist_07       CHAR(24)        NOT NULL,
    s_dist_08       CHAR(24)        NOT NULL,
    s_dist_09       CHAR(24)        NOT NULL,
    s_dist_10       CHAR(24)        NOT NULL,
    s_ytd           NUMERIC(8,0)    NOT NULL,
    s_order_cnt     SMALLINT        NOT NULL,
    s_remote_cnt    SMALLINT        NOT NULL,
    s_data          VARCHAR(50)     NOT NULL,
    PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE "order" (
    o_id            INT             NOT NULL,
    o_d_id          SMALLINT        NOT NULL,
    o_w_id          SMALLINT        NOT NULL,
    o_c_id          INT             NOT NULL,
    o_entry_d       TIMESTAMP       NOT NULL,
    o_carrier_id    SMALLINT,                   -- NULL for undelivered orders
    o_ol_cnt        SMALLINT        NOT NULL,
    o_all_local     SMALLINT        NOT NULL,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

-- Needed by Order-Status (latest order by customer) and Delivery (oldest new-order)
CREATE INDEX idx_order_cust ON "order" (o_w_id, o_d_id, o_c_id, o_id);

CREATE TABLE new_order (
    no_o_id     INT         NOT NULL,
    no_d_id     SMALLINT    NOT NULL,
    no_w_id     SMALLINT    NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE order_line (
    ol_o_id         INT             NOT NULL,
    ol_d_id         SMALLINT        NOT NULL,
    ol_w_id         SMALLINT        NOT NULL,
    ol_number       SMALLINT        NOT NULL,
    ol_i_id         INT             NOT NULL,
    ol_supply_w_id  SMALLINT        NOT NULL,
    ol_delivery_d   TIMESTAMP,                  -- NULL for undelivered lines
    ol_quantity     SMALLINT        NOT NULL,
    ol_amount       NUMERIC(6,2)    NOT NULL,
    ol_dist_info    CHAR(24)        NOT NULL,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);
