/*
 * Copyright 2013 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

----------------------------------------------------------------------------------------------------

/*
 * These parameters default for running the gitrec on a subset of the data on your local machine
 * (see note in README.md about how to get the input data).
 * To run on a cluster, set the s3 locations you want in paramfiles/gitrec-cloud.params
 * and use "mortar:run pigscripts/gitrec.pig -f paramfiles/gitrec-cloud.params"
 */

%default EVENT_LOGS_PATH        '../../../data/github/raw_events/2013/06/{01,02,03,04,05}/*'
%default USER_IDS_PATH          '../../../data/github/user_ids'
%default ITEM_IDS_PATH          '../../../data/github/item_ids'
%default USER_GRAVATAR_IDS_PATH '../../../data/github/user_gravatar_ids'
%default USER_ITEM_SCORES_PATH  '../../../data/github/user_item_scores'
%default ITEM_METADATA_PATH     '../../../data/github/item_metadata'

%default DEFAULT_PARALLEL 1
SET default_parallel $DEFAULT_PARALLEL

REGISTER 'gitrec_udfs.py' USING jython AS udfs;
IMPORT   'utils.pig';

----------------------------------------------------------------------------------------------------

-- load the raw event logs
events          =   LOAD '$EVENT_LOGS_PATH'
                    USING org.apache.pig.piggybank.storage.JsonLoader('
                        actor: chararray,
                        actor_attributes: (
                            gravatar_id: chararray,
                            company: chararray
                        ),
                        repository: (
                            owner: chararray,
                            name: chararray,
                            fork: chararray,
                            language: chararray,
                            description: chararray,
                            watchers: int,
                            stargazers: int,
                            forks: int
                        ),
                        type: chararray,
                        created_at: chararray
                    ');

-- filter to take only events that signal that a user
-- is interested in or a contributor to a repo: Fork, Pull Request, Push, and Watch
events          =   FILTER events BY (
                        (
                            actor            IS NOT NULL AND
                            repository.owner IS NOT NULL AND 
                            repository.name  IS NOT NULL AND
                            repository.fork  IS NOT NULL AND
                            type             IS NOT NULL AND
                            created_at       IS NOT NULL
                        )

                        AND

                        (
                            type == 'ForkEvent'        OR
                            type == 'PullRequestEvent' OR
                            type == 'PushEvent'        OR
                            type == 'WatchEvent'
                        )
                    );

events_renamed  =   FOREACH events GENERATE
                        actor AS user,
                        CONCAT(repository.owner, CONCAT('/', repository.name)) AS item: chararray,
                        repository AS metadata,
                        type,
                        created_at AS timestamp;

-- some logs seem to be malformed and have empty strings
-- for the repository.owner field, so we filter those out
events_renamed  =   FILTER events_renamed BY SUBSTRING(item, 0, 1) != '/';

----------------------------------------------------------------------------------------------------

-- assign an integer id for each user and repo name for better performance
-- we will go back from ids to names when we output final recommendations

user_ids            =   Utils__AssignIntegerIds(events_renamed, 'user');
item_ids            =   Utils__AssignIntegerIds(events_renamed, 'item');

parsed_events       =   FOREACH (JOIN user_ids BY name, events_renamed BY user) GENERATE
                                   id AS user,
                                 item AS item,
                             metadata AS metadata,
                                 type AS type,
                            timestamp AS timestamp;
parsed_events       =   FOREACH (JOIN item_ids BY name, parsed_events BY item) GENERATE
                                 user AS user,
                                   id AS item,
                             metadata AS metadata,
                                 type AS type,
                            timestamp AS timestamp;

-- our model does not use content-based filtering or temporal information, so we throw those out
-- (we show repo metadata from parsed_events for each recommendation at the end however)

parsed_events_trim  =   FOREACH parsed_events GENERATE user, item, type;

----------------------------------------------------------------------------------------------------

-- get the gravatar ids for each user
-- you can get an image from a gravatar id by hitting:
-- http://www.gravatar.com/avatar/[the gravatar id]

events_for_gravatar =   FOREACH (FILTER events BY SIZE(actor_attributes.gravatar_id) == 32) GENERATE
                            actor AS user,
                            actor_attributes.gravatar_id AS gravatar_id,
                            created_at AS timestamp;

latest_by_user      =   FOREACH (GROUP events_for_gravatar BY user) GENERATE
                            FLATTEN(TOP(1, 2, events_for_gravatar))
                            AS (user, gravatar_id, timestamp);

gravatar_ids        =   FOREACH latest_by_user GENERATE user, gravatar_id;

----------------------------------------------------------------------------------------------------

-- give a weighting to each event and aggregate for each unique (user, item) pair
-- to get an "affinity score". Then we apply a logistic scaling function to map every affinity score
-- to a value between 0 and 1, so if a user pushes many many times to a repo,
-- they won't get a unreasonably high affinity score that would mess up later steps in the algorithm.
-- see udfs/jython/gitrec_udfs.py

events_valued   =   FOREACH parsed_events_trim GENERATE
                        user, item, 
                        FLATTEN(udfs.value_event(type))
                        AS (specific_interest, general_interest, graph_score);

ui_totals       =   FOREACH (GROUP events_valued BY (user, item)) GENERATE
                        FLATTEN(group) AS (user, item),
                        (float) SUM(events_valued.specific_interest) AS specific_interest,
                        (float) SUM(events_valued.general_interest)  AS general_interest,
                        (float) SUM(events_valued.graph_score)       AS graph_score;

ui_scores       =   FOREACH ui_totals GENERATE
                        user, item,
                        FLATTEN(udfs.scale_ui_scores(specific_interest, general_interest, graph_score))
                        AS (specific_interest, general_interest, graph_score);

ui_scores       =   FOREACH ui_scores GENERATE
                        user, item,
                        (float) specific_interest,
                        (float) general_interest,
                        (float) graph_score AS score;

-- aggregate affinity scores for each unique repo

item_activities =   FOREACH (GROUP ui_scores BY item) GENERATE
                        group AS item,
                        (float) SUM(ui_scores.score) AS activity;

----------------------------------------------------------------------------------------------------

-- if a user interact with a fork of a repo, in almost all cases
-- it is better to give recommendations based on the original repo for that fork
-- instead of the fork itself.

events_for_fork_map =   FOREACH parsed_events GENERATE
                            item AS item,
                            metadata.name AS repo_name,
                            (metadata.fork == 'true'? 0 : 1) AS is_not_a_fork;

unique_items        =   DISTINCT events_for_fork_map;
original_items      =   FOREACH (GROUP unique_items BY repo_name) GENERATE
                            FLATTEN(TOP(1, 2, unique_items))
                            AS (original_item, repo_name, is_not_a_fork);

fork_map            =   FOREACH (JOIN original_items BY repo_name, unique_items BY repo_name) GENERATE
                            item AS item, original_item AS original_item;

ui_scores           =   FOREACH (JOIN fork_map BY item, ui_scores BY item) GENERATE
                                         user AS user,
                                original_item AS item,
                            specific_interest AS specific_interest,
                             general_interest AS general_interest,
                                        score AS score,
                            (fork_map::item != fork_map::original_item ? 1 : 0) AS mapped_from_fork;

----------------------------------------------------------------------------------------------------

-- we have repo metadata with every event, but we only want the metadata
-- for the most recent state of the repo

most_recent_events  =   FOREACH (GROUP parsed_events BY item) GENERATE
                            FLATTEN(TOP(1, 4, parsed_events))
                            AS (user, item, metadata, type, created_at);

item_metadata       =   FOREACH most_recent_events GENERATE
                            item,
                            (metadata.language is null ? 'Unknown' : metadata.language) AS language,
                            metadata.forks AS num_forks,
                            metadata.stargazers AS num_stars,
                            metadata.description AS description,
                            2 * metadata.forks + metadata.stargazers AS popularity;

-- the "score" field is a combined measure of popularity and activity
-- this is necessary because repos like django-old have lots of stars,
-- but are abandoned, so they should not be considered as recommendations

item_metadata       =   FOREACH (JOIN item_activities BY item, item_metadata BY item) GENERATE
                                            item_metadata::item AS item,
                                                       activity AS activity,
                                                      num_forks AS num_forks,
                                                      num_stars AS num_stars,
                            (float) SQRT(popularity * activity) AS score,
                                                       language AS language,
                                                    description AS description;

----------------------------------------------------------------------------------------------------

rmf $USER_IDS_PATH;
rmf $ITEM_IDS_PATH;
rmf $USER_GRAVATAR_IDS_PATH;
rmf $USER_ITEM_SCORES_PATH;
rmf $ITEM_METADATA_PATH;

STORE user_ids      INTO '$USER_IDS_PATH'          USING PigStorage();
STORE item_ids      INTO '$ITEM_IDS_PATH'          USING PigStorage();
STORE gravatar_ids  INTO '$USER_GRAVATAR_IDS_PATH' USING PigStorage();
STORE ui_scores     INTO '$USER_ITEM_SCORES_PATH'  USING PigStorage();
STORE item_metadata INTO '$ITEM_METADATA_PATH'     USING PigStorage();
