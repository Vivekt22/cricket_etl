from cricket_etl.helpers.catalog import Catalog
import duckdb as db
from cricket_etl.helpers.logger import Logger

logger = Logger("model")


def create_fact_cricket_table(catalog: Catalog):
    # Connect to database
    con = db.connect(catalog.database)

    try:
        con.sql(
            """--sql
            create or replace table model.fact_cricket as
            with t1 as (
                select
                    w.* exclude (
                        match_id,
                        event,
                        event_alias,
                        venue,
                        ground,
                        city,
                        country,
                        gender,
                        match_type,
                        balls_per_over,
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
                        league,
                        batter,
                        batter_id,
                        bowler,
                        bowler_id,
                        non_striker,
                        non_striker_id,
                        player_out,
                        player_out_id,
                        fielder,
                        fielder_id,
                        batting_team,
                        bowling_team,
                        inning_num,
                        wicket_type,
                        batting_order,
                        start_date,
                        end_date
                    ),
                dim_batter.batter_sk,
                dim_bowler.bowler_sk,
                dim_non_striker.non_striker_sk,
                dim_player_out.player_out_sk,
                dim_fielder.fielder_sk,
                dim_batting_team.batting_team_sk,
                dim_bowling_team.bowling_team_sk,
                dim_innings.inning_sk,
                dim_wicket_type.wicket_type_sk,
                dim_batting_order.batting_order_sk,
                dim_match_info.match_sk

                from 
                    wide.cricket w
                    join model.dim_batter on dim_batter.batter_id = w.batter_id
                    join model.dim_bowler on dim_bowler.bowler_id = w.bowler_id
                    join model.dim_non_striker on dim_non_striker.non_striker_id = w.non_striker_id
                    join model.dim_player_out on dim_player_out.player_out_id = w.player_out_id
                    join model.dim_fielder on dim_fielder.fielder_id = w.fielder_id
                    join model.dim_batting_team on dim_batting_team.batting_team = w.batting_team
                    join model.dim_bowling_team on dim_bowling_team.bowling_team = w.bowling_team
                    join model.dim_innings on dim_innings.inning_sk = w.inning_num
                    join model.dim_wicket_type on dim_wicket_type.wicket_type = w.wicket_type
                    join model.dim_batting_order 
                        on dim_batting_order.innings_id = w.innings_id
                        and dim_batting_order.batter_id = w.batter_id
                    join model.dim_match_info on dim_match_info.match_id = w.match_id
            )

            select * from t1
            """
        )

    except Exception as e:
        logger.error(f"Create fact table failed: {e}")
        raise

    finally:
        con.close()

    logger.info("Fact table completed")