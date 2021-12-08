#include <stdio.h>
#include <libpq-fe.h>
#include <string>
#include <fstream>

// yay a bitfield
typedef struct ItemIdData {
    uint32_t    lp_off:15,      /* offset to tuple (from start of page) */
         lp_flags:2,     /* state of line pointer, see below */
        lp_len:15;     /* byte length of tuple */
} ItemIdData;

typedef struct PageHeaderData {
    uint64_t    pd_lsn;
    uint16_t    pd_checksum; 
    uint16_t    pd_flags;
    uint16_t 	pd_lower;
    uint16_t 	pd_upper;
    uint16_t 	pd_special;
    uint16_t	pd_pagesize_version;
    uint32_t	pd_prune_xid;
} PageHeaderData;


// simplified from
// https://github.com/postgres/postgres/blob/master/src/include/access/htup_details.h

struct HeapTupleHeaderData
{
	uint32_t t_xmin;
	uint32_t t_xmax;
	uint32_t t_cid_t_xvac;		/* inserting or deleting command ID, or both */

	 uint32_t ip_blkid;
	 uint16_t ip_posid;

	/* Fields below here must match MinimalTupleData! */

	uint16_t		t_infomask2;	/* number of attributes + various flags */
	uint16_t		t_infomask;		/* various flag bits, see below */
	
	uint8_t		t_hoff;			/* sizeof header incl. bitmap, padding */

	/* ^ - 23 bytes - ^ */

// #define FIELDNO_HEAPTUPLEHEADERDATA_BITS 5
// 	bits8		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

// 	/* MORE DATA FOLLOWS AT END OF STRUCT */
};


int main() {

	assert(sizeof(ItemIdData) == 4);
	assert(sizeof(PageHeaderData) == 24);

	PGconn          *conn;
	PGresult        *res;

	conn = PQconnectdb("");

	if (PQstatus(conn) == CONNECTION_BAD) {
		puts("We were unable to connect to the database");
		exit(0);
	}

	PQexec(conn, "CHECKPOINT");
	PQexec(conn, "SYNC");

	res = PQexec(conn,
		"SELECT setting || '/' || pg_relation_filepath('testtbl') table_path, current_setting('block_size') block_size FROM pg_settings WHERE name = 'data_directory'");

	auto dbfilepath = PQgetvalue(res, 0, 0);
	auto pagesize = atoi(PQgetvalue(res, 0, 1));

	printf("File %s, Page size %d\n", dbfilepath, pagesize);
	PQclear(res);

	std::ifstream is;
	is.open (dbfilepath, std::ios::binary);

	auto page_buf = (char*) malloc(pagesize);
	assert(page_buf);
	auto page_buf_start = page_buf;

	PageHeaderData page_header;
	is.read (page_buf, pagesize);

	memcpy((char*) &page_header, page_buf, sizeof(PageHeaderData));

	page_buf+= sizeof(PageHeaderData);

//	select attnum, attname, attlen, attalign, attnotnull, typname, typlen from pg_attribute join pg_class on attrelid=pg_class.oid join pg_type on atttypid=pg_type.oid where relname='testtbl' and attnum > 0 order by attnum;

// c = char alignment, i.e., no alignment needed.
// s = short alignment (2 bytes on most machines).
// i = int alignment (4 bytes on most machines).
// d = double alignment (8 bytes on many machines, but by no means all).

	printf("pd_lower=%d, pd_upper=%d\n", page_header.pd_lower, page_header.pd_upper);

	assert(page_header.pd_special == pagesize);

	auto n_items = (page_header.pd_lower-24)/4;
	for (uint64_t i = 0; i < n_items; i++) {
				printf("\n");


		ItemIdData item;
		memcpy((char*) &item, page_buf, sizeof(ItemIdData));
		page_buf+= sizeof(ItemIdData);

		printf("lp_off=%d, lp_len=%d\n", item.lp_off, item.lp_len);
		// TODO interpret flags

		assert((item.lp_off + item.lp_len) <= pagesize);

		auto tuple_ptr = page_buf_start  + item.lp_off;
		HeapTupleHeaderData tuple_header;
		memcpy((char*) &tuple_header, tuple_ptr, sizeof(HeapTupleHeaderData));
		// TODO decode the NULL bitmask here, future work
		tuple_ptr+= tuple_header.t_hoff; // TODO we may want to do the varlen thing

		printf("t_xmin=%d, t_xmax=%d, t_hoff=%d\n", tuple_header.t_xmin, tuple_header.t_xmax, tuple_header.t_hoff);


		int32_t val1;
		memcpy((char*) &val1, tuple_ptr, sizeof(int32_t));
		tuple_ptr += sizeof(int32_t);

		// ahaa int64 is stored as int32, int32 not as int64
		tuple_ptr += sizeof(int32_t);

		int32_t val2;
		memcpy((char*) &val2, tuple_ptr, sizeof(int32_t));
		tuple_ptr += sizeof(int32_t);
		

// If it's a variable length field (attlen = -1) then it's a bit more complicated. All variable-length data types share the common header structure struct varlena, which includes the total length of the stored value and some flag bits. Depending on the flags, the data can be either inline or in a TOAST table; it might be compressed, too (see Section 70.2).

		printf("val1=%d, val2=%lld\n", val1, val2);

	}


	PQfinish(conn);
	return 0;
}