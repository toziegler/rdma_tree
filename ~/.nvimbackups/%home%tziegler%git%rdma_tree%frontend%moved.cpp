Vim�UnDo� 0U�����&�Q���a%�E&Աשjj*uV
H   �          Compute<threads::Wor comp;   �                          c��Z    _�                     �       ����                                                                                                                                                                                                                                                                                                                                                             c��r     �   �   �   �             Compute<threads::Wor comp;�         �      *#include "dtree/utils/RandomGenerator.hpp"�   �   �   �            Compute comp;5��    �                     (                     �    �                    (                    �    �                    )                    �    �                    2                    �    �                    2                    �                                        $       �    �                 	   V             	       �    �                     ^                     5�_�                    �       ����                                                                                                                                                                                                                                                                                                                                                             c��{    �   �   �   �      %      Compute<threads::Worker>j comp;5��    �                     ]                     5�_�                    �        ����                                                                                                                                                                                                                                                                                                                                                             c��    �   �   �          Y               threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency,�   �   �          P                  auto success = threads::Worker::my().insert(p_id, key, value);�   �   �          O                  auto found = threads::Worker::my().lookup(p_id, key, rValue);�   �   �          \                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency,�   �   �          _                  auto kv_span = threads::Worker::my().scan(0, start, start + expected_values);�   �   �          h            for (; keep_running; threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p)) {�   �   �          T               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);�   �   �          8               threads::Worker::my().insert(p_id, k, v);�   �   �          ]                t_i, [&, t_i]() { threads::Worker::my().rdma_barrier_wait(barrier_stage); });�   �   �   �      $      Compute<threads::Worker> comp;5��    �                    M                    �    �   "                 |                    �    �                    �                    �    �                    �                    �    �   !                 k                    �    �   !                 �                    �    �                    �                     �    �                    9#                    �    �   !                 >$                    �    �                    �$                    5�_�                    �        ����                                                                                                                                                                                                                                                                                                                                                             c��Y    �   �   �          c               threads::Worker::twosided::my().counters.incr_by(profiling::WorkerCounters::latency,�   �   �          Z                  auto success = threads::Worker::twosided::my().insert(p_id, key, value);�   �   �          Y                  auto found = threads::Worker::twosided::my().lookup(p_id, key, rValue);�   �   �          f                  threads::Worker::twosided::my().counters.incr_by(profiling::WorkerCounters::latency,�   �   �          i                  auto kv_span = threads::Worker::twosided::my().scan(0, start, start + expected_values);�   �   �          r            for (; keep_running; threads::Worker::twosided::my().counters.incr(profiling::WorkerCounters::tx_p)) {�   �   �          ^               threads::Worker::twosided::my().counters.incr(profiling::WorkerCounters::tx_p);�   �   �          B               threads::Worker::twosided::my().insert(p_id, k, v);�   �   �          g                t_i, [&, t_i]() { threads::Worker::twosided::my().rdma_barrier_wait(barrier_stage); });�   �   �   �      .      Compute<threads::Worker::twosided> comp;5��    �                    M                    �    �   "                 |                    �    �                    �                    �    �                    �                    �    �   !                 k                    �    �   !                 �                    �    �                    �                     �    �                    9#                    �    �   !                 >$                    �    �                    �$                    5�_�                    �       ����                                                                                                                                                                                                                                                                                                                                                             c��+    �   �   �   �      8      Compute<threads::twosided::Worker::twosided> comp;5��    �                     V                     �    �                    V                    �    �                    V                    �    �                    V                    �    �   !                 `                    �    �                     _                    5��