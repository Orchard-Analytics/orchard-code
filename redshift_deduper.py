import logging
log = logging.getLogger('Redshift Dedupper')


class redshiftDeduper(object):
    """

    Utility for deduping records in a redshift table.

    Params:
        rs_conn: redshift connection object

        table_name: name of the table (include schema name if needed) to dedupe.

        dedupe_key: the column name to apply deduping to. Usually the primary key but can also accept
                    a concat of columns (e.g. dedupe_key= "concat(user_id, "event_id"))
    """

    def __init__(self, rs_conn, table_name, dedupe_key):
        self.rs_conn = rs_conn
        self.table_name = table_name
        self.dedupe_key = dedupe_key
        self.n_duplicates = self.get_number_duplicates()


    def run(self):
        if self.n_duplicates == 0:
            log.info("No records to dedupe")
        else:
            log.info("Dedupping {} record(s)".format(self.n_duplicates))
            query = self.get_dedupping_query()
            self.rs_conn.execute(query)
            log.info("Done deduping {} record(s) from {}".format(self.n_duplicates, self.table_name))


    def get_dedupping_query(self):
        unique_dupes_temp_table_query = self.get_unique_dupes_temp_table_query()
        delete_dupes_query = self.get_delete_query()
        inert_uniques_query = self.get_insert_unique_dupes_query()

        query = """
        create temp table unique_dupes_temp as (
                {}
            );

            {};
            {};

            drop table unique_dupes_temp;
        """.format(unique_dupes_temp_table_query, delete_dupes_query, inert_uniques_query)
        return query


    def get_insert_unique_dupes_query(self):
        """
            First delete the columns created for deduping: rn and key.
            Then insert the unique rows back in
        """

        query = """
            alter table unique_dupes_temp
            drop column rn;


            alter table unique_dupes_temp
            drop column key;


            insert into {}
                select * from unique_dupes_temp;""".format(self.table_name)
        return query


    def get_delete_query(self):
        query = """
            delete from {}
            where {} in (

              select
                  key
              from unique_dupes_temp
            )""".format(self.table_name, self.dedupe_key)
        return query


    def get_unique_dupes_temp_table_query(self):
        duplicates_query = self.get_duplicates_query()
        row_numbered_dupes_query = self.get_row_number_dupes_query()
        final_uniques_query = self.get_uniques_query()
        query = """
        with
            duplicates as (
                {}
            ),


            dupes_row_numbered as (
                {}
            )


            {}""".format(duplicates_query, row_numbered_dupes_query, final_uniques_query)
        return query


    def get_number_duplicates(self):
        duplicates_query = self.get_duplicates_query()
        return len(self.rs_conn.execute_and_fetch(duplicates_query, return_json=True))


    def get_duplicates_query(self):
        dupe_query = """
                select
                  {} as dedupe_id

                from {}

                group by 1
                having count(*) > 1""".format(self.dedupe_key, self.table_name)
        return dupe_query


    def get_row_number_dupes_query(self):
        dupes_row_numbered_query = """
                with

                  table_with_dedupe_id as (

                      select
                        {} as key
                        , *

                      from {}
                    )


                  select
                    row_number() over (partition by duplicates.dedupe_id) as rn
                    , table_with_dedupe_id.*

                  from table_with_dedupe_id
                  left join duplicates on table_with_dedupe_id.key = duplicates.dedupe_id

                  where duplicates.dedupe_id is not null""".format(self.dedupe_key, self.table_name)
        return dupes_row_numbered_query


    def get_uniques_query(self):
        uniques_query = """
            select
                *
            from dupes_row_numbered
            where rn = 1"""
        return uniques_query