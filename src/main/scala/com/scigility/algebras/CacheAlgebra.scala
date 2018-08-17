package com.scigility.algebras

import cats._
import cats.implicits._
import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref}

trait CacheAlgebra[K,V,F[_]] {
  def get(onMiss: F[V])(k:K):F[V]
}

object CacheAlgebra {

  def concMapCached[K,V,F[_]: Concurrent]: F[CacheAlgebra[K,V,F]] =
    Ref.of[F, Map[K, Deferred[F, V]]](Map.empty[K, Deferred[F, V]]).map(
      cache => new CacheAlgebra[K,V,F] {
        override def get(onMiss: F[V])(k: K): F[V] = Deferred[F, V].flatMap( //TODO build fixed size mechanism for cache. right now this will grow unbounded
          deferred => {
            cache.modify[F[V]](
              state => {
                state.get(k).fold(
                  {
                    val outF = for {
                      res <- onMiss
                      out <- deferred.complete(res) *> deferred.get
                    } yield out

                    (state + (k -> deferred), outF)
                  }
                )(
                  res => (state, res.get)
                )
              }
            ).flatten
          }
        )


      }
    )

}
