from cricket_etl.helpers.catalog import Catalog
import duckdb as db
from cricket_etl.helpers.logger import Logger

logger = Logger("model")

def create_dim_batter(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_batter as
        with t1 as (
            select distinct
                batter,
                batter_id
            from wide.cricket
        )
        select
            row_number() over() as batter_sk,
            *
        from t1
        """
    )

    logger.info("Dim batter completed")

def create_dim_bowler(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_bowler as
        with t1 as (
            select distinct
                bowler,
                bowler_id
            from wide.cricket
        )
        select
            row_number() over() as bowler_sk,
            *
        from t1
        """
    )

    logger.info("Dim bowler completed")

def create_dim_non_striker(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_non_striker as
        with t1 as (
            select distinct
                non_striker,
                non_striker_id
            from wide.cricket
        )
        select
            row_number() over() as non_striker_sk,
            *
        from t1
        """
    )

    logger.info("Dim non striker completed")

def create_dim_player_out(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_player_out as
        with t1 as (
            select distinct
                player_out,
                player_out_id
            from wide.cricket
        )
        select
            row_number() over() as player_out_sk,
            *
        from t1
        """
    )

    logger.info("Dim player out completed")

def create_dim_fielder(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_fielder as
        with t1 as (
            select distinct
                fielder,
                fielder_id
            from wide.cricket
        )
        select
            row_number() over() as fielder_sk,
            *
        from t1
        """
    )

    logger.info("Dim fielder completed")

def create_dim_batting_team(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_batting_team as
        with t1 as (
            select distinct
                batting_team
            from wide.cricket
        )
        select
            row_number() over() as batting_team_sk,
            batting_team
        from t1
        """
    )

    logger.info("Dim batting team completed")

def create_dim_bowling_team(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_bowling_team as
        with t1 as (
            select distinct
                bowling_team
            from wide.cricket
        )
        select
            row_number() over() as bowling_team_sk,
            bowling_team
        from t1
        """
    )

    logger.info("Dim bowling team completed")

def create_dim_innings(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_innings as
        with t1 as (
            select distinct
                inning_num
            from wide.cricket
        )
        select
            inning_num as inning_sk,
            (
                cast(inning_num as varchar)
                || case 
                    when inning_num % 100 between 11 and 13 then 'th'
                    when inning_num % 10 = 1 then 'st'
                    when inning_num % 10 = 2 then 'nd'
                    when inning_num % 10 = 3 then 'rd'
                    else 'th'
                    end 
                || ' Inning'
            ) as inning,
        from t1
        """
    )

    logger.info("Dim innings completed")

def create_dim_wicket_type(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_wicket_type as
        with t1 as (
            select distinct
                wicket_type,
            from wide.cricket
            where wicket_type is not null
            order by wicket_type
        )
        select
            row_number() over () as wicket_type_sk,
            *
        from t1
        union
        select
            -1 as wicket_type_sk,
            'not out' as wicket_type
        order by wicket_type_sk
        """
    )

    logger.info("Dim wicket type completed")

def create_dim_batting_order(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_batting_order as
        with t1 as (
            select distinct
                innings_id,
                batter_id,
                batting_order
            from wide.cricket
        )
        select
            row_number() over() as batting_order_sk,
            *
        from t1
        """
    )

    logger.info("Dim batting order completed")

def create_dim_date(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_date as
        with boundaries as (
            select 
                cast(date_trunc('year', min(start_date)) as date) as start_year,
                cast(date_add(date_trunc('year', max(start_date)), interval 1 year) as date) as next_year
            from wide.cricket
        ),
        t_date_sequence as (
            select generate_series(start_year, next_year, interval 1 day) as d
            from boundaries
        )
        select
            cast(unnest(d) as date) as date,
            year(date) as year,
            month(date) as month,
            monthname(date)[:3] as month_name
        from t_date_sequence
        """
    )

    logger.info("Dim date completed")

def create_dim_match_info(con: db.DuckDBPyConnection):
    con.sql(
        """--sql
        create or replace table model.dim_match_info as
        with t1 as (
            select distinct 
                match_id,
                start_date,
                end_date,
                event as event_ext,
                event_alias as event,
                venue,
                ground,
                city,
                country,
                gender,
                match_type,
                balls_per_over as default_overs,
                match_referee,
                umpire_01,
                umpire_02,
                winner,
                win_by,
                win_margin,
                result,
                player_of_match,
                player_of_match_id,
                season,
                team_01,
                team_02,
                toss_winner,
                toss_decision,
                is_icc_event,
                is_qualifier,
                league
            from wide.cricket
        )
        select
            row_number() over() as match_sk,
            *
        from t1      
        """
    )

    logger.info("Dim match info completed")

def create_dim_tables(catalog: Catalog):
    con = db.connect(catalog.database)
    try:
        create_dim_batter(con)
        create_dim_bowler(con)
        create_dim_non_striker(con)
        create_dim_player_out(con)
        create_dim_fielder(con)
        create_dim_batting_team(con)
        create_dim_bowling_team(con)
        create_dim_innings(con)
        create_dim_wicket_type(con)
        create_dim_batting_order(con)
        create_dim_date(con)
        create_dim_match_info(con)
    except Exception as e:
        logger.error(f"Create dim tables failed: {e}")
        raise
    finally:
        con.close()
