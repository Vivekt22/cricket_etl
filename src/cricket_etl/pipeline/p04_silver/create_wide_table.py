from cricket_etl.helpers.catalog import Catalog
import duckdb as db
from cricket_etl.helpers.logger import Logger

logger = Logger("silver")


def create_wide_table(catalog: Catalog):
    """
    Creates the wide table by first creating a view (v_wide_table)
    from various CTEs and then creating a table wide.cricket.
    
    Parameters:
        con (duckdb.DuckDBPyConnection): The DuckDB connection.
    """
    # Connect to database
    con = db.connect(catalog.database)

    try:
        # Create view for the wide table using CTEs.
        con.sql("use wide")

        con.sql(
            """--sql
            create or replace view v_wide_table_01 as
            
            with cte_01 as (
                select
                    row_number() over (order by start_date, end_date, d.innings_id, d.over, d.ball) as row_id,
                    row_number() over (partition by d.match_id order by d.innings_id, d.over, d.ball) as match_row_id,
                    d.innings_row_index as innings_row_id,
                    d.match_id, 
                    d.innings_id, 
                    i.start_date, 
                    i.end_date, 
                    i.event, 
                    v.venue,
                    v.ground,
                    case
                        when lower(v.city) = 'unknown' then i.city
                        else coalesce(v.city, i.city)
                        end as city,
                    v.country,
                    i.balls_per_over,
                    i.gender, 
                    i.match_type, 
                    i.match_referee, 
                    i.umpire_01, 
                    i.umpire_02,
                    coalesce(i.winner, i.winner_awarded) as winner,
                    i.win_by, 
                    i.win_margin, 
                    i.result,
                    reg_player_of_match.person_name as player_of_match,
                    reg_player_of_match.person_id as player_of_match_id,
                    i.season,
                    i.team_01, 
                    i.team_02, 
                    i.toss_winner, 
                    i.toss_decision,
                    d.batting_team,
                    case 
                        when d.batting_team = i.team_01 then i.team_02
                        else i.team_01
                    end as bowling_team,
                    d.inning_num,
                    d.declared,
                    reg_batter.person_name as batter,
                    reg_batter.person_id as batter_id,
                    reg_bowler.person_name as bowler,
                    reg_bowler.person_id as bowler_id,
                    reg_non_striker.person_name as non_striker,
                    reg_non_striker.person_id as non_striker_id,
                    reg_fielder.person_name as fielder,
                    reg_fielder.person_id as fielder_id,
                    d.over,
                    d.ball,
                    d.batter_runs,
                    coalesce(d.extras, d.extras_1) as extras,
                    d.byes,
                    d.legbyes as leg_byes,
                    d.wides,
                    d.noballs as no_ball,
                    d.penalty,
                    d.total_runs as runs,
                    case when d.batter_runs >= 6 then 1 else 0 end as sixes,
                    case when d.batter_runs = 4 then 1 else 0 end as fours,
                    case 
                        when d.kind is not null and not contains(lower(d.kind), 'retired')
                        then 1 
                        else 0 
                        end as wicket,
                    d.kind as wicket_type,
                    case
                        when contains(lower(d.kind), 'retired')
                        then null
                        else reg_player_out.person_name 
                        end as player_out,
                    case
                        when contains(lower(d.kind), 'retired')
                        then null
                        else reg_player_out.person_id 
                        end as player_out_id,
                    case
                        when contains(lower(event), 'icc') then 1
                        else 0
                        end as is_icc_event,
                    case
                        when contains(lower(event), 'quali') then 1
                        else 0
                        end as is_qualifier,
                    case
                        when contains(lower(event), 'world cup') then 'ICC World Cup'
                        when lower(event) = 'indian premier league' then 'IPL'
                        else event
                        end as event_alias,
                from
                    v_innings d
                    join v_info i using (match_id)
                    left join v_registry reg_batter
                        on reg_batter.match_id = d.match_id
                        and reg_batter.person_name = d.batter
                    left join v_registry reg_bowler
                        on reg_bowler.match_id = d.match_id
                        and reg_bowler.person_name = d.bowler
                    left join v_registry reg_non_striker
                        on reg_non_striker.match_id = d.match_id
                        and reg_non_striker.person_name = d.non_striker
                    left join v_registry reg_player_out
                        on reg_player_out.match_id = d.match_id
                        and reg_player_out.person_name = d.player_out
                    
                    left join v_registry reg_fielder
                        on reg_fielder.match_id = d.match_id
                        and reg_fielder.person_name = d.fielder
                        
                    left join v_registry reg_player_of_match
                        on reg_player_of_match.match_id = d.match_id
                        and reg_player_of_match.person_name = i.player_of_match
                    left join v_venue_map v
                        on v.venue = i.venue
            ),
            cte_02 as (
                select
                    w.* replace(
                        coalesce(map_team_01.team_name_map, w.team_01) as team_01,
                        coalesce(map_team_02.team_name_map, w.team_02) as team_02,
                        coalesce(map_winner.team_name_map, w.winner) as winner,
                        coalesce(map_winner.team_name_map, w.toss_winner) as toss_winner,
                        coalesce(map_batting_team.team_name_map, w.batting_team) as batting_team,
                        coalesce(map_bowling_team.team_name_map, w.bowling_team) as bowling_team
                    ),
                    map_team_01.league as league
                from 
                    cte_01 w
                    left join v_team_league_map map_team_01
                        on map_team_01.team_name = w.team_01
                    left join v_team_league_map map_team_02
                        on map_team_02.team_name = w.team_02
                    left join v_team_league_map map_winner
                        on map_winner.team_name = w.winner
                    left join v_team_league_map map_toss_winner
                        on map_toss_winner.team_name = w.toss_winner
                    left join v_team_league_map map_batting_team
                        on map_batting_team.team_name = w.batting_team
                    left join v_team_league_map map_bowling_team
                        on map_bowling_team.team_name = w.bowling_team
            )
            select *
            from cte_02
            qualify
                count(innings_row_id) over (partition by innings_id, innings_row_id order by row_id) = 1
            order by row_id
            """
        )

        logger.info("Wide table v01 completed")

        # Create batting order
        con.sql(
            """--sql
            create or replace view v_batting_order as
            -- stack the batter and non striker columns
            -- give prio to the batter as this will be used for selected 1 and 2 from the openers
            with pivoted as (
                select
                    innings_id,
                    innings_row_id,
                    batter_id as player_id,
                    1 as batting_end_prio
                from v_wide_table_01
                union all
                select
                    innings_id,
                    innings_row_id,
                    non_striker_id as player_id,
                    2 as batting_end_prio
                from v_wide_table_01
            ),

            -- get the batting order sequence based on which batter appeared first
            ordered as (
                select
                    innings_id,
                    player_id,
                    row_number() over (
                        partition by innings_id 
                        order by innings_row_id, batting_end_prio
                    ) as batting_order_sequence
                from pivoted
            ),

            -- keep the first appearance of each batter
            first_appearance as (
                select
                    innings_id,
                    player_id,
                    min(batting_order_sequence) as batting_order_sequence
                from ordered
                group by all
            )

            -- assign batting order
            select
                innings_id,
                player_id as batter_id,
                row_number() over (
                    partition by innings_id 
                    order by batting_order_sequence
                ) as batting_order
            from first_appearance
            order by innings_id, batting_order
            """
        )

        logger.info("Batting order completed")

        con.sql(
            """--sql
            create or replace view v_wide_table_02 as

            select
                w.*,
                bo.batting_order as batting_order,
            from 
                v_wide_table_01 w
                left join v_batting_order bo
                    on bo.innings_id = w.innings_id
                    and bo.batter_id = w.batter_id  
            """
        )

        # Create schema if it doesn't exist and then create/replace the wide.cricket table.
        con.sql(
            """--sql
            create or replace table wide.cricket as
            select * from v_wide_table_02;
            """
        )

    except Exception as e:
        logger.error(f"Create wide table failed: {e}")
        raise

    finally:
        con.sql("checkpoint cricket")
        con.close()

    logger.info("Create wide table completed")


def write_wide_table_parquet(catalog: Catalog):
    # Connect to database
    con = db.connect(catalog.database)

    # Create view for the wide table using CTEs.
    con.sql("use wide")

    con.sql(
        f"""--sql
        copy (select * from wide.cricket)
        to '{catalog.silver.wide_table}'
        """
    )