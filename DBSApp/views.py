from ast import For
from textwrap import indent
from unittest.mock import patch
from django.http import HttpResponse, JsonResponse
from django.db import connections

def dictfetchall(cursor):
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

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
        cursor.execute(     "SELECT p1.name AS patch_version," +
                                    "CAST(EXTRACT(epoch FROM p1.release_date) AS int) AS patch_start_date," +
                                    "CAST(EXTRACT(epoch FROM p2.release_date) AS int) AS patch_end_date," +
                                    "matches.id AS match_id," +
                                    "CAST(ROUND(CAST(matches.duration AS numeric)/60, 2) AS float) AS duration " +
                                    
                            "FROM patches AS p1 LEFT JOIN patches AS p2 ON p1.id=p2.id-1" +
                            "LEFT JOIN matches ON matches.start_time BETWEEN " +
                            "CAST(EXTRACT(epoch FROM p1.release_date) AS int) AND " +
                            "COALESCE(CAST(EXTRACT(epoch FROM p2.release_date) AS int), 144815040000)" +

                            "ORDER BY CAST(EXTRACT(epoch FROM p1.release_date) AS int), matches.id ASC")
        result = dictfetchall(cursor)

    patchData = {}
    patches = []
    currPatch = ""
    for row in result:
        if row['patch_version'] != currPatch:
            currPatch = row['patch_version']
            if len(patchData) != 0: 
                if(matches[0]['match_id'] == None):
                    patchData['matches'] = []
                else:
                    patchData["matches"] = matches
                patches.append(patchData)
            patchData = {
                "patch_version":row['patch_version'],
                "patch_start_date":row['patch_start_date'],
                "patch_end_date":row['patch_end_date']
            }
            matches = []

        matches.append({
            "match_id":row['match_id'],
            "duration":row['duration']
        })
    #the last patch
    if len(patchData) != 0: 
        if(matches[0]['match_id'] == None):
            patchData['matches'] = []
        else:
            patchData["matches"] = matches
        patches.append(patchData)

    JSON = {
        "patches": patches
    }
    cursor.close()

    return HttpResponse(JsonResponse(JSON), status=200)

def xp(request, player_id):
    with connections['db'].cursor() as cursor:
        cursor.execute(
            "SELECT  players.id," +   
                    "COALESCE(players.nick,'unknown') AS player_nick," +
                    "heroes.localized_name AS hero_localized_name," +
                    "ROUND(CAST(matches.duration AS numeric)/60, 2) AS match_duration_minutes," +
                    "(COALESCE(matches_players_details.xp_hero, 0) +" +
                    "COALESCE(matches_players_details.xp_creep, 0) +" +
                    "COALESCE(matches_players_details.xp_other, 0) +" +
                    "COALESCE(matches_players_details.xp_roshan, 0)) AS experiences_gained," +
                    "matches_players_details.level AS level_gained," +
                    "CASE " +
                        "WHEN radiant_win=true AND player_slot BETWEEN 0 AND 4 THEN true " +
                        "WHEN radiant_win=true AND player_slot BETWEEN 128 AND 132 THEN false " +
                        "WHEN radiant_win=false AND player_slot BETWEEN 0 AND 4 THEN false " +
                        "WHEN radiant_win=false AND player_slot BETWEEN 128 AND 132 THEN true " +
                    "END AS winner," +
                    "matches.id AS match_id " +
            "FROM players 	JOIN matches_players_details ON players.id=matches_players_details.player_id " +
                            "JOIN matches ON matches_players_details.match_id=matches.id " +
                            "JOIN heroes ON matches_players_details.hero_id=heroes.id " +
            "WHERE players.id=" + str(player_id))
        result = dictfetchall(cursor)

    JSON = {
        "id":result[0]["id"],
        "player_nick":result[0]["player_nick"]
    }

    matches = []
    for row in result:
        matchData = {}
        matchData['match_id'] = row["match_id"]
        matchData['hero_localized_name'] = row["hero_localized_name"]
        matchData['match_duration_minutes'] = row["match_duration_minutes"]
        matchData['experiences_gained'] = row["experiences_gained"]
        matchData['level_gained'] = row["level_gained"]
        matchData['winner'] = row["winner"]
        matches.append(matchData)
    
    JSON['matches'] = matches
    
    cursor.close()
    return HttpResponse(JsonResponse(JSON), status=200)

def objectives(request, player_id):
    with connections['db'].cursor() as cursor:
        cursor.execute(
            "SELECT DISTINCT " +
                    "players.id," +
                    "players.nick AS player_nick," +
                    "heroes.localized_name AS hero_localized_name," +
                    "matches.id AS match_id," +
                    "COALESCE(gameOb.subtype, 'NO_ACTION') AS hero_action," +
                    "COUNT(gameOB.subtype) AS count " +
            "FROM players " +
            "JOIN matches_players_details AS mpd ON players.id=mpd.player_id " +
            "JOIN heroes ON mpd.hero_id=heroes.id " +
            "JOIN matches ON mpd.match_id=matches.id " +
            "LEFT JOIN game_objectives AS gameOb ON mpd.id=gameOb.match_player_detail_id_1 " +
            "WHERE players.id=" + str(player_id) + " " +
            "GROUP BY(players.id," +
                    "players.nick," +
                    "heroes.localized_name," +
                    "matches.id," +
                    "gameOb.subtype) " +
            "ORDER BY players.id, matches.id")
        result = dictfetchall(cursor)

    JSON = {
        "id":result[0]["id"],
        "nick":result[0]["player_nick"]
    }

    matches = []
    matchData = {}
    actionData = []
    currMatchID = ""
    for row in result:
        if(currMatchID != row['match_id']):
            currMatchID = row['match_id']

            if(len(actionData) != 0):
                if(actionData[0]['hero_action'] == None):
                    matchData['hero_action'] = []
                else:
                    matchData['hero_action'] = actionData

                matches.append(matchData)

            matchData = {
                "match_id":row["match_id"],
                "hero_localized_name":row["hero_localized_name"]
            }
            actionData = []

        actionData.append({
            "hero_action":row["hero_action"],
            "count":row["count"]
        })
    #the last match data
    if(len(actionData) != 0):
        if(actionData[0]['hero_action'] == None):
            matchData['hero_action'] = []
        else:
            matchData['hero_action'] = actionData

        matches.append(matchData)

    
    JSON['matches'] = matches
    
    cursor.close()
    return HttpResponse(JsonResponse(JSON), status=200)

def abilities(request, player_id):
    with connections['db'].cursor() as cursor:
        cursor.execute(
            "SELECT DISTINCT " +
                    "players.id," +
                    "COALESCE(players.nick,'unknown') AS player_nick," +
                    "heroes.localized_name AS hero_localized_name," +
                    "matches.id AS match_id," +
                    "abilities.name AS ability_name," +
                    "count(abilities.name) AS count," +
                    "MAX(au.level) AS upgrade_level " +
            "FROM players " +
            "JOIN matches_players_details AS mpd ON mpd.player_id=players.id " +
            "JOIN matches ON mpd.match_id=matches.id " +
            "JOIN heroes ON mpd.hero_id=heroes.id " +
            "JOIN ability_upgrades AS au ON au.match_player_detail_id=mpd.id " +
            "JOIN abilities ON au.ability_id=abilities.id " +
            "WHERE players.id=" + str(player_id) + " " +
            "GROUP BY 	players.id," +
                        "players.nick," +
                        "heroes.localized_name," +
                        "matches.id," +
                        "abilities.name " +
            "ORDER BY matches.id")
        result = dictfetchall(cursor)

    JSON = {
        "id":result[0]["id"],
        "nick":result[0]["player_nick"]
    }

    matches = []
    matchData = {}
    abilityData = []
    currMatchID = ""
    for row in result:
        if(currMatchID != row['match_id']):
            currMatchID = row['match_id']

            if(len(abilityData) != 0):
                if(abilityData[0]['ability_name'] == None):
                    matchData['abilities'] = []
                else:
                    matchData['abilities'] = abilityData

                matches.append(matchData)

            matchData = {
                "match_id":row["match_id"],
                "hero_localized_name":row["hero_localized_name"]
            }
            abilityData = []

        abilityData.append({
            "ability_name":row["ability_name"],
            "count":row["count"],
            "upgrade_level":row["upgrade_level"]
        })

    #the last match data
    if(len(abilityData) != 0):
        if(abilityData[0]['ability_name'] == None):
            matchData['abilities'] = []
        else:
            matchData['abilities'] = abilityData

        matches.append(matchData)
    
    JSON['matches'] = matches

    cursor.close()
    return HttpResponse(JsonResponse(JSON), status=200)

#level gained set to 0 if NO action in /objectives
#isnt the last patch data missing in patches (the last append is not executed in for imo)