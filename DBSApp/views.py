from textwrap import indent
from django.http import HttpResponse, JsonResponse
from django.db import connections

def base(request):
    return HttpResponse("<html><body>Hello World!</body></html>")

def zad2(request):
    with connections['db'].cursor() as cursor:
        cursor.execute("SELECT VERSION();")
        version = cursor.fetchone()
        cursor.execute("SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size;")
        dbSize = cursor.fetchone()

        JSON = {
            "pgsql":{
                "version" : version[0],
                "dota2_db_size" : dbSize[0]
            }
        }
        
        cursor.close()

    return HttpResponse(JsonResponse(JSON), content_type="application/json")

def patches(request):
    with connections['db'].cursor() as cursor:
        """
        FINAL 1
        SELECT 	p1.name AS patch_name,
		CAST(EXTRACT(epoch FROM p1.release_date) AS int) AS patch_start_date, 
		CAST(EXTRACT(epoch FROM p2.release_date) AS int) AS patch_end_date,
		matches.id AS match_id, 
		ROUND(CAST(matches.duration AS numeric)/60, 2) AS duration
FROM patches AS p1 LEFT JOIN patches AS p2 ON p1.id=p2.id-1
LEFT JOIN matches ON matches.start_time BETWEEN 
CAST(EXTRACT(epoch FROM p1.release_date) AS int) AND CAST(EXTRACT(epoch FROM p2.release_date) AS int)
ORDER BY CAST(EXTRACT(epoch FROM p1.release_date) AS int) ASC"""
        """
        FINAL FINAL 1
        SELECT 	p1.name AS patch_name,
		CAST(EXTRACT(epoch FROM p1.release_date) AS int) AS patch_start_date, 
		CAST(EXTRACT(epoch FROM p2.release_date) AS int) AS patch_end_date,
		matches.id AS match_id, 
		ROUND(CAST(matches.duration AS numeric)/60, 2) AS duration
FROM patches AS p1 LEFT JOIN patches AS p2 ON p1.id=p2.id-1
LEFT JOIN matches ON matches.start_time BETWEEN 
CAST(EXTRACT(epoch FROM p1.release_date) AS int) AND 
COALESCE(CAST(EXTRACT(epoch FROM p2.release_date) AS int), 144815040000)/*2 extra 0 digits for good measure*/
ORDER BY CAST(EXTRACT(epoch FROM p1.release_date) AS int) DESC
        """

        
        cursor.execute(     "SELECT p1.name AS patch_name," +
                                    "CAST(EXTRACT(epoch FROM p1.release_date) AS int) AS patch_start_date," +
                                    "CAST(EXTRACT(epoch FROM p2.release_date) AS int) AS patch_end_date," +
                                    "matches.id AS match_id," +
                                    "ROUND(CAST(matches.duration AS numeric)/60, 2) AS duration" +
                                    
                            "FROM patches AS p1 LEFT JOIN patches AS p2 ON p1.id=p2.id-1" +
                            "LEFT JOIN matches ON matches.start_time BETWEEN " +
                            "CAST(EXTRACT(epoch FROM p1.release_date) AS int) AND " +
                            "COALESCE(CAST(EXTRACT(epoch FROM p2.release_date) AS int), 144815040000)" +

                            "ORDER BY CAST(EXTRACT(epoch FROM p1.release_date) AS int) DESC")
        result = cursor.fetchone()

    JSON = {}
    return HttpResponse(JsonResponse(JSON), content="application/json", status=200)


    """SECOND OK but sloooow: SELECT 	players.id, 
		COALESCE(players.nick,'unknown') AS player_nick, 
		heroes.localized_name AS hero_localized_name, 
		matches.duration AS match_duration_minutes, 
		(matches_players_details.xp_hero +
		matches_players_details.xp_creep +
		matches_players_details.xp_other +
		matches_players_details.xp_roshan) AS experiences_gained, 
		matches_players_details.level AS level_gained, 
		CASE 
			WHEN radiant_win=true AND player_slot BETWEEN 0 AND 4 THEN true
			WHEN radiant_win=true AND player_slot BETWEEN 128 AND 132 THEN false
			WHEN radiant_win=false AND player_slot BETWEEN 0 AND 4 THEN false
			WHEN radiant_win=false AND player_slot BETWEEN 128 AND 132 THEN true
		END AS winner,
		matches.id AS match_id
FROM players 	JOIN matches_players_details ON players.id=matches_players_details.player_id
				JOIN matches ON matches_players_details.match_id=matches.id
				JOIN heroes ON matches_players_details.hero_id=heroes.id"""

    """FINAL 2
    SELECT 	players.id, 
		COALESCE(players.nick,'unknown') AS player_nick, 
		heroes.localized_name AS hero_localized_name, 
		ROUND(CAST(matches.duration AS numeric)/60, 2) AS match_duration_minutes, 
		(COALESCE(matches_players_details.xp_hero, 0) +
		COALESCE(matches_players_details.xp_creep, 0) +
		COALESCE(matches_players_details.xp_other, 0) +
		COALESCE(matches_players_details.xp_roshan, 0)) AS experiences_gained, 
		matches_players_details.level AS level_gained, 
		CASE 
			WHEN radiant_win=true AND player_slot BETWEEN 0 AND 4 THEN true
			WHEN radiant_win=true AND player_slot BETWEEN 128 AND 132 THEN false
			WHEN radiant_win=false AND player_slot BETWEEN 0 AND 4 THEN false
			WHEN radiant_win=false AND player_slot BETWEEN 128 AND 132 THEN true
		END AS winner,
		matches.id AS match_id
FROM players 	JOIN matches_players_details ON players.id=matches_players_details.player_id
				JOIN matches ON matches_players_details.match_id=matches.id
				JOIN heroes ON matches_players_details.hero_id=heroes.id
WHERE players.id=14944
    """
    """
    FINAL 3
    SELECT DISTINCT
		players.id,
		players.nick,
		heroes.localized_name,
		matches.id AS match_id,
		COALESCE(gameOb.subtype, 'NO_ACTION') AS actions,
		COUNT(gameOB.subtype)
FROM players
JOIN matches_players_details AS mpd ON players.id=mpd.player_id
JOIN heroes ON mpd.hero_id=heroes.id
JOIN matches ON mpd.match_id=matches.id
LEFT JOIN game_objectives AS gameOb ON mpd.id=gameOb.match_player_detail_id_1
WHERE players.id=14944
GROUP BY(players.id,
		players.nick,
		heroes.localized_name,
		matches.id,
		gameOb.subtype)
ORDER BY players.id, matches.id

    """

    """
    FINAL 4
    SELECT DISTINCT
		players.id,
		players.nick,
		heroes.localized_name,
		matches.id AS match_id,
		abilities.name AS ability_name,
		count(abilities.name),
		MAX(au.level)
FROM players
JOIN matches_players_details AS mpd ON mpd.player_id=players.id
JOIN matches ON mpd.match_id=matches.id
JOIN heroes ON mpd.hero_id=heroes.id
JOIN ability_upgrades AS au ON au.match_player_detail_id=mpd.id
JOIN abilities ON au.ability_id=abilities.id
WHERE players.id=14944
GROUP BY 	players.id,
			players.nick,
			heroes.localized_name,
			matches.id,
			abilities.name
ORDER BY matches.id
    """