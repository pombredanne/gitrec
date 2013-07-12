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

REGISTER 'datafu-0.0.10.jar';
DEFINE Utils__EnumerateFromOne datafu.pig.bags.Enumerate('1');

----------------------------------------------------------------------------------------------------

/*
 * events: {anything as long as it has the field entity_field refers to}
 * entity_field: name of the field to assign ids to
 * -->
 * id_map: {id: int, name: chararray}
 */
DEFINE Utils__AssignIntegerIds(events, entity_field)
returns id_map {
    grouped             =   GROUP $events BY $entity_field;
    names               =   FOREACH grouped GENERATE group AS name;
    enumerated          =   FOREACH (GROUP names ALL) GENERATE
                                FLATTEN(Utils__EnumerateFromOne(names))
                                AS (name, id);
    $id_map             =   FOREACH enumerated GENERATE id, name;
};