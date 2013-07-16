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

IMPORT 'matrix.pig';

----------------------------------------------------------------------------------------------------

/*
 * All graphs are represented as sparse adjacency matrices: { row: int, col: int, val: float }
 */

/*
 * Adds an edge of zero weight from each vertex in a graph to itself.
 *
 * graph: {row: int, col: int, val: float}
 * -->
 * out_graph: {row: int, col: int, val: float}
 * vertices:  {id: int}
 */
DEFINE Graph__AddSelfLoops(graph)
RETURNS out_graph, vertices {
    from_vertices       =   FOREACH $graph GENERATE row AS id;
    to_vertices         =   FOREACH $graph GENERATE col AS id;
    vertices_with_dups  =   UNION from_vertices, to_vertices;
    $vertices           =   DISTINCT vertices_with_dups;
    self_loops          =   FOREACH $vertices GENERATE id AS row, id AS col, 0.0f AS val;
    $out_graph          =   UNION self_loops, $graph;
};

/*
 * Given a graph, returns a new graph with edges from each vertex in the original graph
 * to the neighborhood_size closest vertices in its 2-neighborhood by shortest path distance.
 * Implemented using Matrix Min-Plus Multiplication.
 *
 * graph: {row: int, col: int, val: float}
 * neighborhood_size: int
 * -->
 * nhoods: {row: int, col: int, val: float}
 */
DEFINE Graph__ShortestPaths_TwoSteps(graph, neighborhood_size)
RETURNS nhoods {
    graph, vertices     =   Graph__AddSelfLoops($graph);
    squared             =   Matrix__MinPlusSquared(graph);
    squared_trimmed     =   Matrix__TrimRows(squared, 'ASC', $neighborhood_size);
    $nhoods             =   FILTER squared_trimmed BY row != col;
};

/*
 * Given a graph, returns a new graph with edges from each vertex in the original graph
 * to the neighborhood_size closest vertices in its 3-neighborhood by shortest path distance.
 * Implemented using Matrix Min-Plus Multiplication.
 *
 * graph: {row: int, col: int, val: float}
 * neighborhood_size: int
 * -->
 * nhoods: {row: int, col: int, val: float}
 */
DEFINE Graph__ShortestPaths_ThreeSteps(graph, neighborhood_size)
RETURNS nhoods {
    graph, vertices     =   Graph__AddSelfLoops($graph);
    squared             =   Matrix__MinPlusSquared(graph);
    squared_trimmed     =   Matrix__TrimRows(squared, 'ASC', $neighborhood_size);
    cubed               =   Matrix__MinPlusProduct(squared_trimmed, graph);
    cubed_trimmed       =   Matrix__TrimRows(cubed, 'ASC', $neighborhood_size);
    $nhoods             =   FILTER cubed_trimmed BY row != col;
};

----------------------------------------------------------------------------------------------------

/*
 * To initialize a random walk, we normalize the values for each row in its adjacency matrix,
 * meaning that the walk has a probability at each vertex to follow one of its incident edges
 * to a neighboring vertex proportional to the weight of that edge.
 *
 * This macro also returns a copy of this transition matrix under a different alias.
 * This is needed for the first calling of Graph__RandomWalkStep.
 *
 * graph: {row: int, col: int, val: float}
 * -->
 * walk_graph: {row: int, col: int, val: float}
 */
DEFINE Graph__RandomWalk_Init(graph)
RETURNS trans_mat, trans_mat_copy {
    $trans_mat          =   Matrix__NormalizeRows($graph);
    $trans_mat_copy     =   FOREACH $trans_mat GENERATE *;
};

/*
 * Given a graph representing the current state of a random walk,
 * the initial transition matrix for the walk, and a "neighborhood size"
 * (only the neighborhood_size most probable paths will be followed for performance reasons),
 * return the transition state matrix for the next step of the walk.
 *
 * prev_walk_step:    {row: int, col: int, val: float}
 * trans_mat:         {row: int, col: int, val: float},
 * neighborhood_size: int
 * -->
 * new_walk_step: {row: int, col: int, val: float}
 */
DEFINE Graph__RandomWalk_Step(prev_walk_step, trans_mat, neighborhood_size)
RETURNS new_walk_step {
    new_trans_mat       =   Matrix__Product($prev_walk_step, $trans_mat);
    $new_walk_step      =   Matrix__TrimRows(new_trans_mat, 'DESC', $neighborhood_size);
};

/*
 * Completes a random walk. The random walk macros add self-loops
 * (i.e. path A -> B -> A becomes edge A -> A after one step),
 * so this filters them out and renormalizes each row to be a transition probability.
 *
 * final_walk_step: {row: int, col: int, val: float}
 * -->
 * walk_result: {row: int, col: int, val: float}
 */
DEFINE Graph__RandomWalk_Complete(final_walk_step)
RETURNS walk_result {
    no_self_loops       =   FOREACH (FILTER $final_walk_step BY row != col) GENERATE row, col, val;
    $walk_result        =   Matrix__NormalizeRows(no_self_loops);
};

----------------------------------------------------------------------------------------------------

/*
 * Used by Personalized Pagerank, this implements the chance at each step in the random walk
 * of "teleporting" back to the start node. While regular Pagerank teleports to a random node,
 * by teleporting back to each individual walk's starting node, Personalized Pagerank
 * ensures that vertices close to the source node usually get a weighting then vertices far away.
 *
 * prev_walk_step: {row: int, col: int, val: float}
 * teleport_prob:  float
 * -->
 * new_walk_step: {row: int, col: int, val: float}
 */
DEFINE Graph__RandomWalk_TeleportToSource(prev_walk_step, teleport_prob)
RETURNS new_walk_step {
    damped              =   FOREACH $prev_walk_step GENERATE
                                row, col,
                                (1.0 - $teleport_prob) * val AS val;
    sources             =   FOREACH $prev_walk_step GENERATE row;
    teleports           =   FOREACH (DISTINCT sources) GENERATE
                                row, row AS col, $teleport_prob AS val;
    $new_walk_step      =   UNION damped, teleports;
};

/*
 * Same as Graph__RandomWalk_Step, except with the added teleport step.
 *
 * prev_walk_step:    {row: int, col: int, val: float}
 * trans_mat:         {row: int, col: int, val: float},
 * teleport_prob:     float
 * neighborhood_size: int
 * -->
 * think_of_home (the new walk step): {row: int, col: int, val: float}
 */
DEFINE Graph__PersonalizedPagerank_Iterate(prev_walk_step, trans_mat, teleport_prob, neighborhood_size)
RETURNS think_of_home {
    go_on_an_adventure  =     Graph__RandomWalk_Step($prev_walk_step, $trans_mat, $neighborhood_size);
    $think_of_home      =     Graph__RandomWalk_TeleportToSource(go_on_an_adventure, $teleport_prob);
};
