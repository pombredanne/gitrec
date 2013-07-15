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
DEFINE Utils__Enumerate datafu.pig.bags.Enumerate('1');

----------------------`------------------------------------------------------------------------------

/*
 * Enumerates the distinct values of one field from a relation, producing unique integer ids.
 * You will have to join this id map back to your original data to convert representations.
 *
 * We wanted to implement this generically in the macro, but there were problems maintaining
 * the schema of the other fields in the input data.
 *
 * data: {anything as long as it has the field name_field refers to}
 * name_field: name of the field to assign ids to
 * -->
 * id_map: {id: int, name: chararray}
 */
DEFINE Utils__AssignIntegerIds(data, name_field)
RETURNS id_map {
    names           =   FOREACH $data GENERATE $name_field;
    names           =   DISTINCT names;
    enumerated      =   FOREACH (GROUP names ALL) GENERATE
                            FLATTEN(Utils__Enumerate(names))
                            AS (name, id);
    $id_map         =   FOREACH enumerated GENERATE id, name;
};
