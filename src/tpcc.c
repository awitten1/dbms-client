/*
 * tpcc.c — TPC-C benchmark client using libpq
 *
 * Usage:
 *   tpcc load [-w warehouses] [-h host] [-p port] [-U user] [-d dbname] [--schema path]
 *   tpcc run  [-w warehouses] [-c clients] [-T seconds] [-h host] [-p port] [-U user] [-d dbname]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <libpq-fe.h>

/* ── Constants ───────────────────────────────────────────────────────────── */

#define N_ITEMS             100000
#define N_DIST              10          /* districts per warehouse */
#define N_CUST              3000        /* customers per district */
#define N_ORDERS            3000        /* initial orders per district */
#define N_NEW_ORDERS        900         /* last 900 orders are undelivered */
#define N_NEWORDER_START    (N_ORDERS - N_NEW_ORDERS + 1)

/* Cumulative transaction mix percentages */
#define MIX_NEWORDER        45
#define MIX_PAYMENT         88
#define MIX_ORDSTATUS       92
#define MIX_DELIVERY        96
/* Stock-Level: 97-100 */

/* SQL name for the orders table — ORDER is a reserved keyword */
#define OT "\"order\""

/* ── NURand C-constants (initialized once in main before threads) ─────────── */

static int g_c_last;
static int g_c_id;
static int g_ol_iid;

/* ── Structs ─────────────────────────────────────────────────────────────── */

typedef struct {
    char host[64];
    char port[8];
    char user[64];
    char dbname[64];
    char schema_path[256];
    int  warehouses;
    int  clients;
    int  duration;
} Config;

typedef struct {
    PGconn          *conn;
    Config          *cfg;
    int              id;
    long             ok[5];
    long             err[5];
    volatile int     stop;
} Worker;

/* ── Random utilities ────────────────────────────────────────────────────── */

static const char *SYLL[10] = {
    "BAR","OUGHT","ABLE","PRI","PRES","ESE","ANTI","CALLY","ATION","EING"
};

static int urand(int lo, int hi) {
    return lo + (int)((double)rand() / ((double)RAND_MAX + 1) * (hi - lo + 1));
}

static int nurand(int A, int C, int x, int y) {
    return (((urand(0, A) | urand(x, y)) + C) % (y - x + 1)) + x;
}

static void gen_last(int n, char *buf) {
    buf[0] = '\0';
    strcat(buf, SYLL[n / 100]);
    strcat(buf, SYLL[(n / 10) % 10]);
    strcat(buf, SYLL[n % 10]);
}

static void rand_astr(char *buf, int lo, int hi) {
    static const char alpha[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    int len = urand(lo, hi);
    for (int i = 0; i < len; i++)
        buf[i] = alpha[urand(0, (int)(sizeof(alpha) - 2))];
    buf[len] = '\0';
}

static void rand_nstr(char *buf, int len) {
    for (int i = 0; i < len; i++) buf[i] = '0' + urand(0, 9);
    buf[len] = '\0';
}

/* Random data field; 10% of the time embeds "ORIGINAL" */
static void rand_data(char *buf, int lo, int hi) {
    rand_astr(buf, lo, hi);
    if (urand(1, 10) == 1) {
        int n = (int)strlen(buf);
        if (n >= 8)
            memcpy(buf + urand(0, n - 8), "ORIGINAL", 8);
    }
}

/* ── Connection helpers ──────────────────────────────────────────────────── */

static PGconn *db_connect(const Config *cfg) {
    char cs[256];
    /* Always TCP — never Unix socket. Required for network stack profiling. */
    snprintf(cs, sizeof(cs), "host=%s port=%s user=%s dbname=%s",
             cfg->host, cfg->port, cfg->user, cfg->dbname);
    PGconn *conn = PQconnectdb(cs);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }
    return conn;
}

/* ── COPY helpers ────────────────────────────────────────────────────────── */

static void copy_begin(PGconn *conn, const char *sql) {
    PGresult *r = PQexec(conn, sql);
    if (PQresultStatus(r) != PGRES_COPY_IN) {
        fprintf(stderr, "COPY start failed: %s\n", PQresultErrorMessage(r));
        PQclear(r); PQfinish(conn); exit(1);
    }
    PQclear(r);
}

static void copy_row(PGconn *conn, const char *row) {
    if (PQputCopyData(conn, row, (int)strlen(row)) != 1) {
        fprintf(stderr, "PQputCopyData: %s\n", PQerrorMessage(conn));
        PQfinish(conn); exit(1);
    }
}

static void copy_end(PGconn *conn) {
    if (PQputCopyEnd(conn, NULL) != 1) {
        fprintf(stderr, "PQputCopyEnd: %s\n", PQerrorMessage(conn));
        PQfinish(conn); exit(1);
    }
    PGresult *r = PQgetResult(conn);
    if (PQresultStatus(r) != PGRES_COMMAND_OK) {
        fprintf(stderr, "COPY end: %s\n", PQresultErrorMessage(r));
        PQclear(r); PQfinish(conn); exit(1);
    }
    PQclear(r);
}

/* ── Loaders ─────────────────────────────────────────────────────────────── */

static void load_item(PGconn *conn) {
    printf("  item...\n");
    copy_begin(conn,
        "COPY item(i_id,i_im_id,i_name,i_price,i_data)"
        " FROM STDIN WITH(FORMAT text,DELIMITER E'\\t',NULL '\\N')");
    char row[128], name[25], data[51];
    for (int i = 1; i <= N_ITEMS; i++) {
        rand_astr(name, 14, 24);
        rand_data(data, 26, 50);
        snprintf(row, sizeof(row), "%d\t%d\t%s\t%.2f\t%s\n",
                 i, urand(1, 10000), name, urand(100, 10000) / 100.0, data);
        copy_row(conn, row);
    }
    copy_end(conn);
}

static void load_warehouse(PGconn *conn, int w) {
    copy_begin(conn,
        "COPY warehouse(w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,w_ytd)"
        " FROM STDIN WITH(FORMAT text,DELIMITER E'\\t',NULL '\\N')");
    char row[256], name[11], s1[21], s2[21], city[21], state[3], zip[10];
    rand_astr(name, 6, 10); rand_astr(s1, 10, 20); rand_astr(s2, 10, 20);
    rand_astr(city, 10, 20); rand_astr(state, 2, 2);
    rand_nstr(zip, 4); strcat(zip, "11111");
    snprintf(row, sizeof(row), "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%.4f\t300000.00\n",
             w, name, s1, s2, city, state, zip, urand(0, 2000) / 10000.0);
    copy_row(conn, row);
    copy_end(conn);
}

static void load_district(PGconn *conn, int w) {
    copy_begin(conn,
        "COPY district(d_id,d_w_id,d_name,d_street_1,d_street_2,d_city,"
        "d_state,d_zip,d_tax,d_ytd,d_next_o_id)"
        " FROM STDIN WITH(FORMAT text,DELIMITER E'\\t',NULL '\\N')");
    char row[256], name[11], s1[21], s2[21], city[21], state[3], zip[10];
    for (int d = 1; d <= N_DIST; d++) {
        rand_astr(name, 6, 10); rand_astr(s1, 10, 20); rand_astr(s2, 10, 20);
        rand_astr(city, 10, 20); rand_astr(state, 2, 2);
        rand_nstr(zip, 4); zip[4] = '\0'; strcat(zip, "11111");
        snprintf(row, sizeof(row),
                 "%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%.4f\t30000.00\t%d\n",
                 d, w, name, s1, s2, city, state, zip,
                 urand(0, 2000) / 10000.0, N_ORDERS + 1);
        copy_row(conn, row);
    }
    copy_end(conn);
}

static void load_customer(PGconn *conn, int w) {
    printf("  customers w=%d...\n", w);
    copy_begin(conn,
        "COPY customer(c_id,c_d_id,c_w_id,c_first,c_middle,c_last,"
        "c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,"
        "c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,"
        "c_payment_cnt,c_delivery_cnt,c_data)"
        " FROM STDIN WITH(FORMAT text,DELIMITER E'\\t',NULL '\\N')");
    char row[1024], first[17], last[17], s1[21], s2[21];
    char city[21], state[3], zip[10], phone[17], data[501];
    for (int d = 1; d <= N_DIST; d++) {
        for (int c = 1; c <= N_CUST; c++) {
            rand_astr(first, 8, 16);
            gen_last(c <= 1000 ? c - 1 : nurand(255, g_c_last, 0, 999), last);
            rand_astr(s1, 10, 20); rand_astr(s2, 10, 20);
            rand_astr(city, 10, 20); rand_astr(state, 2, 2);
            rand_nstr(zip, 4); zip[4] = '\0'; strcat(zip, "11111");
            rand_nstr(phone, 16);
            rand_astr(data, 300, 500);
            const char *credit = (urand(1, 10) == 1) ? "BC" : "GC";
            snprintf(row, sizeof(row),
                "%d\t%d\t%d\t%s\tOE\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
                "\tnow\t%s\t50000.00\t%.4f\t-10.00\t10.00\t1\t0\t%s\n",
                c, d, w, first, last, s1, s2, city, state, zip, phone,
                credit, urand(0, 5000) / 10000.0, data);
            copy_row(conn, row);
        }
    }
    copy_end(conn);
}

static void load_history(PGconn *conn, int w) {
    copy_begin(conn,
        "COPY history(h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data)"
        " FROM STDIN WITH(FORMAT text,DELIMITER E'\\t',NULL '\\N')");
    char row[128], data[25];
    for (int d = 1; d <= N_DIST; d++) {
        for (int c = 1; c <= N_CUST; c++) {
            rand_astr(data, 12, 24);
            snprintf(row, sizeof(row), "%d\t%d\t%d\t%d\t%d\tnow\t10.00\t%s\n",
                     c, d, w, d, w, data);
            copy_row(conn, row);
        }
    }
    copy_end(conn);
}

static void load_stock(PGconn *conn, int w) {
    printf("  stock w=%d...\n", w);
    copy_begin(conn,
        "COPY stock(s_i_id,s_w_id,s_quantity,"
        "s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,"
        "s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,"
        "s_ytd,s_order_cnt,s_remote_cnt,s_data)"
        " FROM STDIN WITH(FORMAT text,DELIMITER E'\\t',NULL '\\N')");
    char row[512], ds[10][25], data[51];
    for (int i = 1; i <= N_ITEMS; i++) {
        for (int k = 0; k < 10; k++) rand_astr(ds[k], 24, 24);
        rand_data(data, 26, 50);
        snprintf(row, sizeof(row),
            "%d\t%d\t%d"
            "\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
            "\t0\t0\t0\t%s\n",
            i, w, urand(10, 100),
            ds[0],ds[1],ds[2],ds[3],ds[4],ds[5],ds[6],ds[7],ds[8],ds[9],
            data);
        copy_row(conn, row);
    }
    copy_end(conn);
}

/*
 * load_orders — populates "order", new_order, and order_line tables.
 *
 * Challenge: orders and order_line are naturally generated together (each
 * order has ol_cnt lines), but COPY only allows one active stream per
 * connection at a time. Solution: three sequential passes per district,
 * caching ol_cnt and the customer permutation in small arrays.
 *
 *   Pass 1a: "order"     — writes o_ol_cnt; caches ol_cnt[0..2999]
 *   Pass 1b: new_order   — o_id >= N_NEWORDER_START
 *   Pass 2:  order_line  — reads cached ol_cnt to match row counts
 */
static void load_orders(PGconn *conn, int w) {
    printf("  orders w=%d...\n", w);
    int ol_cnt[N_ORDERS];
    int cperm[N_CUST];
    char row[256], dist_info[25];

    for (int d = 1; d <= N_DIST; d++) {
        /* Fisher-Yates shuffle for customer assignment per spec 4.3.3.1 */
        for (int i = 0; i < N_CUST; i++) cperm[i] = i + 1;
        for (int i = N_CUST - 1; i > 0; i--) {
            int j = urand(0, i);
            int t = cperm[i]; cperm[i] = cperm[j]; cperm[j] = t;
        }
        for (int o = 0; o < N_ORDERS; o++)
            ol_cnt[o] = urand(5, 15);

        /* Pass 1a: "order" */
        copy_begin(conn,
            "COPY " OT "(o_id,o_d_id,o_w_id,o_c_id,o_entry_d,"
            "o_carrier_id,o_ol_cnt,o_all_local)"
            " FROM STDIN WITH(FORMAT text,DELIMITER E'\\t',NULL '\\N')");
        for (int o = 1; o <= N_ORDERS; o++) {
            if (o < N_NEWORDER_START)
                snprintf(row, sizeof(row), "%d\t%d\t%d\t%d\tnow\t%d\t%d\t1\n",
                         o, d, w, cperm[o-1], urand(1, 10), ol_cnt[o-1]);
            else
                snprintf(row, sizeof(row), "%d\t%d\t%d\t%d\tnow\t\\N\t%d\t1\n",
                         o, d, w, cperm[o-1], ol_cnt[o-1]);
            copy_row(conn, row);
        }
        copy_end(conn);

        /* Pass 1b: new_order */
        copy_begin(conn,
            "COPY new_order(no_o_id,no_d_id,no_w_id)"
            " FROM STDIN WITH(FORMAT text,DELIMITER E'\\t',NULL '\\N')");
        for (int o = N_NEWORDER_START; o <= N_ORDERS; o++) {
            snprintf(row, sizeof(row), "%d\t%d\t%d\n", o, d, w);
            copy_row(conn, row);
        }
        copy_end(conn);

        /* Pass 2: order_line */
        copy_begin(conn,
            "COPY order_line(ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,"
            "ol_supply_w_id,ol_delivery_d,ol_quantity,ol_amount,ol_dist_info)"
            " FROM STDIN WITH(FORMAT text,DELIMITER E'\\t',NULL '\\N')");
        for (int o = 1; o <= N_ORDERS; o++) {
            int delivered = (o < N_NEWORDER_START);
            for (int ol = 1; ol <= ol_cnt[o-1]; ol++) {
                rand_astr(dist_info, 24, 24);
                if (delivered)
                    snprintf(row, sizeof(row),
                             "%d\t%d\t%d\t%d\t%d\t%d\tnow\t5\t%.2f\t%s\n",
                             o, d, w, ol, urand(1, N_ITEMS), w,
                             urand(1, 999999) / 100.0, dist_info);
                else
                    snprintf(row, sizeof(row),
                             "%d\t%d\t%d\t%d\t%d\t%d\t\\N\t5\t0.00\t%s\n",
                             o, d, w, ol, urand(1, N_ITEMS), w, dist_info);
                copy_row(conn, row);
            }
        }
        copy_end(conn);
    }
}

static void run_load(Config *cfg) {
    PGconn *conn = db_connect(cfg);

    /* Apply schema */
    FILE *f = fopen(cfg->schema_path, "r");
    if (!f) { perror(cfg->schema_path); exit(1); }
    fseek(f, 0, SEEK_END); long sz = ftell(f); rewind(f);
    char *schema = malloc((size_t)sz + 1);
    (void)fread(schema, 1, (size_t)sz, f);
    schema[sz] = '\0';
    fclose(f);

    printf("Applying schema from %s...\n", cfg->schema_path);
    PGresult *r = PQexec(conn, schema);
    free(schema);
    if (PQresultStatus(r) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Schema error: %s\n", PQresultErrorMessage(r));
        PQclear(r); PQfinish(conn); exit(1);
    }
    PQclear(r);

    printf("Loading %d warehouse(s)...\n", cfg->warehouses);
    load_item(conn);
    for (int w = 1; w <= cfg->warehouses; w++) {
        printf("Warehouse %d/%d:\n", w, cfg->warehouses);
        load_warehouse(conn, w);
        load_district(conn, w);
        load_customer(conn, w);
        load_history(conn, w);
        load_stock(conn, w);
        load_orders(conn, w);
    }
    printf("Load complete.\n");
    PQfinish(conn);
}

/* ── Transaction helpers ─────────────────────────────────────────────────── */

static int tx_begin(PGconn *conn) {
    PGresult *r = PQexec(conn, "BEGIN");
    int ok = (PQresultStatus(r) == PGRES_COMMAND_OK);
    PQclear(r);
    return ok ? 0 : -1;
}

static int tx_commit(PGconn *conn) {
    PGresult *r = PQexec(conn, "COMMIT");
    int ok = (PQresultStatus(r) == PGRES_COMMAND_OK);
    PQclear(r);
    return ok ? 0 : -1;
}

static void tx_rollback(PGconn *conn) {
    PGresult *r = PQexec(conn, "ROLLBACK");
    PQclear(r);
}

/*
 * Run a parameterized query. Returns the result on success, NULL on error.
 * On error, prints the error (unless it's a serialization error) and rolls
 * back the transaction. Caller must PQclear() the returned result.
 */
static PGresult *qparam(PGconn *conn, const char *sql, int n,
                        const char * const *params, ExecStatusType exp)
{
    PGresult *r = PQexecParams(conn, sql, n, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(r) != exp) {
        const char *ss = PQresultErrorField(r, PG_DIAG_SQLSTATE);
        if (!ss || (strcmp(ss, "40001") != 0 && strcmp(ss, "40P01") != 0))
            fprintf(stderr, "query error [%s]: %s\n",
                    ss ? ss : "?", PQresultErrorMessage(r));
        PQclear(r);
        tx_rollback(conn);
        return NULL;
    }
    return r;
}

/* ── Transactions ────────────────────────────────────────────────────────── */

static int tx_new_order(Worker *wk) {
    PGconn *conn = wk->conn;
    PGresult *r;
    char pw[8], pd[8], pc[8], po[8], pol[8], pi[8], pqty[8], pnq[8], pamt[24];

    int w_id   = urand(1, wk->cfg->warehouses);
    int d_id   = urand(1, N_DIST);
    int c_id   = nurand(1023, g_c_id, 1, N_CUST);
    int ol_cnt = urand(5, 15);

    snprintf(pw, sizeof(pw), "%d", w_id);
    snprintf(pd, sizeof(pd), "%d", d_id);
    snprintf(pc, sizeof(pc), "%d", c_id);

    if (tx_begin(conn) < 0) return -1;

    /* 1. Warehouse tax */
    const char *p1[] = {pw};
    r = qparam(conn, "SELECT w_tax FROM warehouse WHERE w_id=$1",
               1, p1, PGRES_TUPLES_OK);
    if (!r) return -1;
    PQclear(r);

    /* 2. District: get next_o_id and lock */
    const char *p2[] = {pw, pd};
    r = qparam(conn,
               "SELECT d_tax,d_next_o_id FROM district"
               " WHERE d_w_id=$1 AND d_id=$2 FOR UPDATE",
               2, p2, PGRES_TUPLES_OK);
    if (!r) return -1;
    int o_id = atoi(PQgetvalue(r, 0, 1));
    PQclear(r);
    snprintf(po, sizeof(po), "%d", o_id);

    /* 3. Increment next_o_id */
    r = qparam(conn,
               "UPDATE district SET d_next_o_id=d_next_o_id+1"
               " WHERE d_w_id=$1 AND d_id=$2",
               2, p2, PGRES_COMMAND_OK);
    if (!r) return -1;
    PQclear(r);

    /* 4. Customer */
    const char *p3[] = {pw, pd, pc};
    r = qparam(conn,
               "SELECT c_discount,c_last,c_credit FROM customer"
               " WHERE c_w_id=$1 AND c_d_id=$2 AND c_id=$3",
               3, p3, PGRES_TUPLES_OK);
    if (!r) return -1;
    PQclear(r);

    /* 5. Insert order */
    char pol_cnt[8]; snprintf(pol_cnt, sizeof(pol_cnt), "%d", ol_cnt);
    const char *p4[] = {po, pd, pw, pc, pol_cnt, "1"};
    r = qparam(conn,
               "INSERT INTO " OT
               "(o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_ol_cnt,o_all_local)"
               " VALUES($1,$2,$3,$4,NOW(),$5,$6)",
               6, p4, PGRES_COMMAND_OK);
    if (!r) return -1;
    PQclear(r);

    /* 6. Insert new_order */
    const char *p5[] = {po, pd, pw};
    r = qparam(conn,
               "INSERT INTO new_order(no_o_id,no_d_id,no_w_id) VALUES($1,$2,$3)",
               3, p5, PGRES_COMMAND_OK);
    if (!r) return -1;
    PQclear(r);

    /* 7. Order lines */
    for (int ol = 1; ol <= ol_cnt; ol++) {
        int i_id    = nurand(8191, g_ol_iid, 1, N_ITEMS);
        int qty     = urand(1, 10);
        snprintf(pi,  sizeof(pi),  "%d", i_id);
        snprintf(pol, sizeof(pol), "%d", ol);
        snprintf(pqty,sizeof(pqty),"%d", qty);

        /* 7a. Item */
        const char *pai[] = {pi};
        r = qparam(conn,
                   "SELECT i_price,i_name,i_data FROM item WHERE i_id=$1",
                   1, pai, PGRES_TUPLES_OK);
        if (!r) return -1;
        if (PQntuples(r) == 0) {
            /* 1% rollback: item not found (simulated via i_id >= N_ITEMS) */
            PQclear(r); tx_rollback(conn); return 0;
        }
        double i_price = atof(PQgetvalue(r, 0, 0));
        PQclear(r);

        /* 7b. Stock — column name varies with district */
        char sql[192];
        snprintf(sql, sizeof(sql),
                 "SELECT s_quantity,s_dist_%02d,s_data FROM stock"
                 " WHERE s_w_id=$1 AND s_i_id=$2 FOR UPDATE", d_id);
        const char *pas[] = {pw, pi};
        r = qparam(conn, sql, 2, pas, PGRES_TUPLES_OK);
        if (!r) return -1;
        int s_qty = atoi(PQgetvalue(r, 0, 0));
        PQclear(r);

        int new_qty = (s_qty - qty < 10) ? s_qty - qty + 91 : s_qty - qty;
        snprintf(pnq, sizeof(pnq), "%d", new_qty);
        const char *pus[] = {pnq, pqty, pw, pi};
        r = qparam(conn,
                   "UPDATE stock SET s_quantity=$1,s_ytd=s_ytd+$2,"
                   "s_order_cnt=s_order_cnt+1"
                   " WHERE s_w_id=$3 AND s_i_id=$4",
                   4, pus, PGRES_COMMAND_OK);
        if (!r) return -1;
        PQclear(r);

        /* 7c. Insert order_line */
        snprintf(pamt, sizeof(pamt), "%.2f", qty * i_price);
        const char *paol[] = {po, pd, pw, pol, pi, pw, pqty, pamt, "dist_info_placeholder"};
        r = qparam(conn,
                   "INSERT INTO order_line"
                   "(ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,"
                   "ol_supply_w_id,ol_quantity,ol_amount,ol_dist_info)"
                   " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                   9, paol, PGRES_COMMAND_OK);
        if (!r) return -1;
        PQclear(r);
    }

    return tx_commit(conn);
}

static int tx_payment(Worker *wk) {
    PGconn *conn = wk->conn;
    PGresult *r;
    char pw[8], pd[8], pcw[8], pcd[8], pc[8], pamt[24];

    int w_id   = urand(1, wk->cfg->warehouses);
    int d_id   = urand(1, N_DIST);
    int c_w_id = w_id;
    int c_d_id = d_id;
    int c_id   = nurand(1023, g_c_id, 1, N_CUST);
    double h_amount = urand(100, 500000) / 100.0;

    snprintf(pw,   sizeof(pw),   "%d", w_id);
    snprintf(pd,   sizeof(pd),   "%d", d_id);
    snprintf(pcw,  sizeof(pcw),  "%d", c_w_id);
    snprintf(pcd,  sizeof(pcd),  "%d", c_d_id);
    snprintf(pc,   sizeof(pc),   "%d", c_id);
    snprintf(pamt, sizeof(pamt), "%.2f", h_amount);

    if (tx_begin(conn) < 0) return -1;

    /* 1. Update warehouse YTD */
    const char *p1[] = {pamt, pw};
    r = qparam(conn, "UPDATE warehouse SET w_ytd=w_ytd+$1 WHERE w_id=$2",
               2, p1, PGRES_COMMAND_OK);
    if (!r) return -1; PQclear(r);

    /* 2. Get warehouse */
    const char *p2[] = {pw};
    r = qparam(conn,
               "SELECT w_name,w_street_1,w_street_2,w_city,w_state,w_zip"
               " FROM warehouse WHERE w_id=$1",
               1, p2, PGRES_TUPLES_OK);
    if (!r) return -1;
    char w_name[11];
    strncpy(w_name, PQgetvalue(r, 0, 0), 10); w_name[10] = '\0';
    PQclear(r);

    /* 3. Update district YTD */
    const char *p3[] = {pamt, pw, pd};
    r = qparam(conn,
               "UPDATE district SET d_ytd=d_ytd+$1 WHERE d_w_id=$2 AND d_id=$3",
               3, p3, PGRES_COMMAND_OK);
    if (!r) return -1; PQclear(r);

    /* 4. Get district */
    const char *p4[] = {pw, pd};
    r = qparam(conn,
               "SELECT d_name,d_street_1,d_street_2,d_city,d_state,d_zip"
               " FROM district WHERE d_w_id=$1 AND d_id=$2",
               2, p4, PGRES_TUPLES_OK);
    if (!r) return -1;
    char d_name[11];
    strncpy(d_name, PQgetvalue(r, 0, 0), 10); d_name[10] = '\0';
    PQclear(r);

    /* 5. Get customer */
    const char *p5[] = {pcw, pcd, pc};
    r = qparam(conn,
               "SELECT c_first,c_last,c_credit,c_balance FROM customer"
               " WHERE c_w_id=$1 AND c_d_id=$2 AND c_id=$3 FOR UPDATE",
               3, p5, PGRES_TUPLES_OK);
    if (!r) return -1;
    const char *credit = PQgetvalue(r, 0, 2);
    int is_bc = (strncmp(credit, "BC", 2) == 0);
    PQclear(r);

    /* 6. Update customer */
    if (is_bc) {
        /* Bad credit: append h_data to c_data (truncated to 500 chars) */
        char new_data[512];
        snprintf(new_data, sizeof(new_data), "%s %s %s %s %s %.2f",
                 pc, pcd, pcw, pd, pw, h_amount);
        const char *p6[] = {pamt, pc, pcd, pcw, new_data};
        r = qparam(conn,
                   "UPDATE customer SET c_balance=c_balance-$1,"
                   "c_ytd_payment=c_ytd_payment+$1,c_payment_cnt=c_payment_cnt+1,"
                   "c_data=LEFT($5||c_data,500)"
                   " WHERE c_id=$2 AND c_d_id=$3 AND c_w_id=$4",
                   5, p6, PGRES_COMMAND_OK);
    } else {
        const char *p6[] = {pamt, pc, pcd, pcw};
        r = qparam(conn,
                   "UPDATE customer SET c_balance=c_balance-$1,"
                   "c_ytd_payment=c_ytd_payment+$1,c_payment_cnt=c_payment_cnt+1"
                   " WHERE c_id=$2 AND c_d_id=$3 AND c_w_id=$4",
                   4, p6, PGRES_COMMAND_OK);
    }
    if (!r) return -1; PQclear(r);

    /* 7. Insert history */
    char h_data[25];
    snprintf(h_data, sizeof(h_data), "%.10s    %.10s", w_name, d_name);
    const char *p7[] = {pc, pcd, pcw, pd, pw, pamt, h_data};
    r = qparam(conn,
               "INSERT INTO history(h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,"
               "h_date,h_amount,h_data) VALUES($1,$2,$3,$4,$5,NOW(),$6,$7)",
               7, p7, PGRES_COMMAND_OK);
    if (!r) return -1; PQclear(r);

    return tx_commit(conn);
}

static int tx_order_status(Worker *wk) {
    PGconn *conn = wk->conn;
    PGresult *r;
    char pw[8], pd[8], pc[8];

    int w_id = urand(1, wk->cfg->warehouses);
    int d_id = urand(1, N_DIST);
    int c_id = nurand(1023, g_c_id, 1, N_CUST);

    snprintf(pw, sizeof(pw), "%d", w_id);
    snprintf(pd, sizeof(pd), "%d", d_id);
    snprintf(pc, sizeof(pc), "%d", c_id);

    if (tx_begin(conn) < 0) return -1;

    /* 1. Customer */
    const char *p1[] = {pw, pd, pc};
    r = qparam(conn,
               "SELECT c_first,c_middle,c_last,c_balance FROM customer"
               " WHERE c_w_id=$1 AND c_d_id=$2 AND c_id=$3",
               3, p1, PGRES_TUPLES_OK);
    if (!r) return -1; PQclear(r);

    /* 2. Most recent order */
    r = qparam(conn,
               "SELECT o_id,o_entry_d,o_carrier_id FROM " OT
               " WHERE o_w_id=$1 AND o_d_id=$2 AND o_c_id=$3"
               " ORDER BY o_id DESC LIMIT 1",
               3, p1, PGRES_TUPLES_OK);
    if (!r) return -1;
    if (PQntuples(r) == 0) { PQclear(r); tx_rollback(conn); return 0; }
    char po[8];
    snprintf(po, sizeof(po), "%s", PQgetvalue(r, 0, 0));
    PQclear(r);

    /* 3. Order lines */
    const char *p2[] = {pw, pd, po};
    r = qparam(conn,
               "SELECT ol_number,ol_i_id,ol_supply_w_id,ol_quantity,"
               "ol_amount,ol_delivery_d FROM order_line"
               " WHERE ol_w_id=$1 AND ol_d_id=$2 AND ol_o_id=$3",
               3, p2, PGRES_TUPLES_OK);
    if (!r) return -1; PQclear(r);

    return tx_commit(conn);
}

static int tx_delivery(Worker *wk) {
    PGconn *conn = wk->conn;
    PGresult *r;
    char pw[8], pd[8], po[8], pcarr[8];

    int w_id       = urand(1, wk->cfg->warehouses);
    int carrier_id = urand(1, 10);

    snprintf(pw,   sizeof(pw),   "%d", w_id);
    snprintf(pcarr,sizeof(pcarr),"%d", carrier_id);

    if (tx_begin(conn) < 0) return -1;

    for (int d = 1; d <= N_DIST; d++) {
        snprintf(pd, sizeof(pd), "%d", d);

        /* 1. Oldest new_order */
        const char *p1[] = {pw, pd};
        r = qparam(conn,
                   "SELECT no_o_id FROM new_order"
                   " WHERE no_w_id=$1 AND no_d_id=$2"
                   " ORDER BY no_o_id LIMIT 1 FOR UPDATE",
                   2, p1, PGRES_TUPLES_OK);
        if (!r) return -1;
        if (PQntuples(r) == 0) { PQclear(r); continue; }
        snprintf(po, sizeof(po), "%s", PQgetvalue(r, 0, 0));
        PQclear(r);

        /* 2. Delete new_order */
        const char *p2[] = {pw, pd, po};
        r = qparam(conn,
                   "DELETE FROM new_order WHERE no_w_id=$1 AND no_d_id=$2 AND no_o_id=$3",
                   3, p2, PGRES_COMMAND_OK);
        if (!r) return -1; PQclear(r);

        /* 3. Get customer from order */
        r = qparam(conn,
                   "SELECT o_c_id FROM " OT
                   " WHERE o_w_id=$1 AND o_d_id=$2 AND o_id=$3",
                   3, p2, PGRES_TUPLES_OK);
        if (!r) return -1;
        char pc[8];
        snprintf(pc, sizeof(pc), "%s", PQgetvalue(r, 0, 0));
        PQclear(r);

        /* 4. Update order carrier */
        const char *p3[] = {pcarr, pw, pd, po};
        r = qparam(conn,
                   "UPDATE " OT " SET o_carrier_id=$1"
                   " WHERE o_w_id=$2 AND o_d_id=$3 AND o_id=$4",
                   4, p3, PGRES_COMMAND_OK);
        if (!r) return -1; PQclear(r);

        /* 5. Update order_line delivery date */
        r = qparam(conn,
                   "UPDATE order_line SET ol_delivery_d=NOW()"
                   " WHERE ol_w_id=$1 AND ol_d_id=$2 AND ol_o_id=$3",
                   3, p2, PGRES_COMMAND_OK);
        if (!r) return -1; PQclear(r);

        /* 6. Sum order_line amounts */
        r = qparam(conn,
                   "SELECT SUM(ol_amount) FROM order_line"
                   " WHERE ol_w_id=$1 AND ol_d_id=$2 AND ol_o_id=$3",
                   3, p2, PGRES_TUPLES_OK);
        if (!r) return -1;
        char psum[32];
        snprintf(psum, sizeof(psum), "%s", PQgetvalue(r, 0, 0));
        PQclear(r);

        /* 7. Update customer */
        const char *p4[] = {psum, pw, pd, pc};
        r = qparam(conn,
                   "UPDATE customer SET c_balance=c_balance+$1,"
                   "c_delivery_cnt=c_delivery_cnt+1"
                   " WHERE c_w_id=$2 AND c_d_id=$3 AND c_id=$4",
                   4, p4, PGRES_COMMAND_OK);
        if (!r) return -1; PQclear(r);
    }

    return tx_commit(conn);
}

static int tx_stock_level(Worker *wk) {
    PGconn *conn = wk->conn;
    PGresult *r;
    char pw[8], pd[8], pthr[8];

    int w_id      = urand(1, wk->cfg->warehouses);
    int d_id      = urand(1, N_DIST);
    int threshold = urand(10, 20);

    snprintf(pw,   sizeof(pw),   "%d", w_id);
    snprintf(pd,   sizeof(pd),   "%d", d_id);
    snprintf(pthr, sizeof(pthr), "%d", threshold);

    if (tx_begin(conn) < 0) return -1;

    /* 1. Get next_o_id */
    const char *p1[] = {pw, pd};
    r = qparam(conn,
               "SELECT d_next_o_id FROM district WHERE d_w_id=$1 AND d_id=$2",
               2, p1, PGRES_TUPLES_OK);
    if (!r) return -1;
    int next_o_id = atoi(PQgetvalue(r, 0, 0));
    PQclear(r);

    /* 2. Count distinct low-stock items in last 20 orders */
    char plo[8], phi[8];
    snprintf(plo, sizeof(plo), "%d", next_o_id - 20);
    snprintf(phi, sizeof(phi), "%d", next_o_id - 1);
    const char *p2[] = {pw, pd, plo, phi, pthr};
    r = qparam(conn,
               "SELECT COUNT(DISTINCT s_i_id) FROM order_line"
               " JOIN stock ON ol_i_id=s_i_id AND ol_w_id=s_w_id"
               " WHERE ol_w_id=$1 AND ol_d_id=$2"
               " AND ol_o_id BETWEEN $3 AND $4"
               " AND s_quantity < $5",
               5, p2, PGRES_TUPLES_OK);
    if (!r) return -1; PQclear(r);

    return tx_commit(conn);
}

/* ── Worker thread ───────────────────────────────────────────────────────── */

static const char *TX_NAMES[] = {
    "NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"
};

static void *worker_thread(void *arg) {
    Worker *wk = arg;
    srand((unsigned)(time(NULL) ^ (unsigned long)pthread_self()));

    while (!wk->stop) {
        int r    = urand(1, 100);
        int type = (r <= MIX_NEWORDER)  ? 0 :
                   (r <= MIX_PAYMENT)   ? 1 :
                   (r <= MIX_ORDSTATUS) ? 2 :
                   (r <= MIX_DELIVERY)  ? 3 : 4;
        int ret;
        switch (type) {
            case 0: ret = tx_new_order(wk);    break;
            case 1: ret = tx_payment(wk);      break;
            case 2: ret = tx_order_status(wk); break;
            case 3: ret = tx_delivery(wk);     break;
            default:ret = tx_stock_level(wk);  break;
        }
        if (ret == 0) wk->ok[type]++;
        else          wk->err[type]++;
    }
    return NULL;
}

static void run_bench(Config *cfg) {
    Worker   *workers = calloc((size_t)cfg->clients, sizeof(Worker));
    pthread_t *tids   = malloc((size_t)cfg->clients * sizeof(pthread_t));

    for (int i = 0; i < cfg->clients; i++) {
        workers[i].conn = db_connect(cfg);
        workers[i].cfg  = cfg;
        workers[i].id   = i;
        workers[i].stop = 0;
    }

    printf("Running TPC-C: %d warehouse(s), %d client(s), %ds\n",
           cfg->warehouses, cfg->clients, cfg->duration);

    time_t start = time(NULL);
    for (int i = 0; i < cfg->clients; i++)
        pthread_create(&tids[i], NULL, worker_thread, &workers[i]);

    /* Progress reports from main thread */
    while (time(NULL) - start < cfg->duration) {
        sleep(10);
        long total = 0;
        for (int i = 0; i < cfg->clients; i++)
            for (int t = 0; t < 5; t++) total += workers[i].ok[t];
        long elapsed = time(NULL) - start;
        if (elapsed > 0)
            printf("[%3lds] TPS: %.1f\n", elapsed, (double)total / elapsed);
    }

    for (int i = 0; i < cfg->clients; i++) workers[i].stop = 1;
    for (int i = 0; i < cfg->clients; i++) pthread_join(tids[i], NULL);

    long elapsed = time(NULL) - start;
    long grand_ok = 0;
    long tx_ok[5] = {0}, tx_err[5] = {0};
    for (int i = 0; i < cfg->clients; i++) {
        for (int t = 0; t < 5; t++) {
            tx_ok[t]  += workers[i].ok[t];
            tx_err[t] += workers[i].err[t];
            grand_ok  += workers[i].ok[t];
        }
        PQfinish(workers[i].conn);
    }

    printf("\n--- TPC-C Results (%lds) ---\n", elapsed);
    for (int t = 0; t < 5; t++)
        printf("  %-15s  ok: %6ld  err: %3ld\n", TX_NAMES[t], tx_ok[t], tx_err[t]);
    if (elapsed > 0)
        printf("  Total TPS: %.2f\n", (double)grand_ok / elapsed);

    free(workers);
    free(tids);
}

/* ── main ────────────────────────────────────────────────────────────────── */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s <command> [options]\n"
        "\n"
        "Commands:\n"
        "  load   Load TPC-C initial data\n"
        "  run    Run TPC-C benchmark\n"
        "\n"
        "Options:\n"
        "  -w <warehouses>   Scale factor (default: 1)\n"
        "  -c <clients>      Parallel clients for run (default: 1)\n"
        "  -T <seconds>      Run duration (default: 60)\n"
        "  -h <host>         DB host (default: localhost)\n"
        "  -p <port>         DB port (default: 5432)\n"
        "  -U <user>         DB user (default: $USER)\n"
        "  -d <dbname>       Database (default: bench)\n"
        "  -s <path>         Schema SQL path for load (default: sql/tpcc_schema.sql)\n",
        prog);
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(argv[0]); return 1; }

    Config cfg = {
        .host       = "localhost",
        .port       = "5432",
        .dbname     = "bench",
        .schema_path= "sql/tpcc_schema.sql",
        .warehouses = 1,
        .clients    = 1,
        .duration   = 60,
    };

    /* Default user */
    const char *env_user = getenv("USER");
    if (env_user) strncpy(cfg.user, env_user, sizeof(cfg.user) - 1);

    const char *cmd = argv[1];
    int opt;
    optind = 2; /* skip past the command */
    while ((opt = getopt(argc, argv, "w:c:T:h:p:U:d:s:")) != -1) {
        switch (opt) {
            case 'w': cfg.warehouses = atoi(optarg); break;
            case 'c': cfg.clients    = atoi(optarg); break;
            case 'T': cfg.duration   = atoi(optarg); break;
            case 'h': strncpy(cfg.host,   optarg, sizeof(cfg.host)  - 1); break;
            case 'p': strncpy(cfg.port,   optarg, sizeof(cfg.port)  - 1); break;
            case 'U': strncpy(cfg.user,   optarg, sizeof(cfg.user)  - 1); break;
            case 'd': strncpy(cfg.dbname, optarg, sizeof(cfg.dbname)- 1); break;
            case 's': strncpy(cfg.schema_path, optarg, sizeof(cfg.schema_path) - 1); break;
            default:  usage(argv[0]); return 1;
        }
    }

    /* Initialize NURand C-constants once before any threads start */
    srand((unsigned)time(NULL));
    g_c_last = urand(0, 255);
    g_c_id   = urand(0, 1023);
    g_ol_iid = urand(0, 8191);

    if (strcmp(cmd, "load") == 0) {
        run_load(&cfg);
    } else if (strcmp(cmd, "run") == 0) {
        run_bench(&cfg);
    } else {
        fprintf(stderr, "Unknown command: %s\n", cmd);
        usage(argv[0]);
        return 1;
    }

    return 0;
}
