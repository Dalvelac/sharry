package sharry.backend.share

import cats.effect.*

import sharry.backend.PasswordCrypt
import sharry.common.*
import sharry.store.*
import sharry.store.records.*

import binny.ByteRange
import munit.*
import scodec.bits.ByteVector

class QueriesTest extends FunSuite with StoreFixture {

  private val cfg = ShareConfig(
    chunkSize = ByteSize(64 * 1024),
    maxSize = ByteSize(0),
    maxValidity = Duration.days(30),
    databaseDomainChecks = Nil,
    zipMaxSize = ByteSize(0),
    requireSharePassword = false
  )

  private val publishId = Ident.unsafe("pub1")
  private val fileId = Ident.unsafe("file1")

  /** Inserts one published share with a single file. The publish window is open (until
    * tomorrow) so the only thing that can close it in these tests is maxViews.
    */
  private def insertShare(
      store: Store[IO],
      maxViews: Int,
      password: Option[Password]
  ): IO[Unit] =
    Timestamp.current[IO].flatMap { now =>
      val accountId = Ident.unsafe("acc1")
      val shareId = Ident.unsafe("share1")
      val metaId = Ident.unsafe("meta1")

      val account = RAccount(
        accountId,
        CIIdent.unsafe("jdoe"),
        AccountSource.intern,
        AccountState.Active,
        Password("test"),
        Some("test@test.com"),
        admin = true,
        0,
        None,
        now
      )
      val share = RShare(
        shareId,
        accountId,
        None,
        None,
        Duration.hours(1),
        maxViews,
        password.map(PasswordCrypt.crypt),
        None,
        now
      )
      val meta =
        RFileMeta(metaId, now, "application/octet-stream", ByteSize(0), ByteVector.empty)
      val shareFile =
        RShareFile(fileId, shareId, metaId, Some("file.txt"), now, ByteSize(0))
      val publish = RPublishShare(
        publishId,
        shareId,
        enabled = true,
        views = 0,
        lastAccess = None,
        publishDate = now,
        publishUntil = now.plus(Duration.days(1)),
        created = now
      )

      store.transact(RAccount.insert(account, "warn")) *>
        store.transact(RShare.insert(share)) *>
        store.transact(RFileMeta.insert(meta)) *>
        store.transact(RShareFile.insert(shareFile)) *>
        store.transact(RPublishShare.insert(publish)).void
    }

  private def addView(store: Store[IO]): IO[Unit] =
    store.transact(Queries.countPublishAccess(ShareId.publish(publishId)))

  private def loadFile(store: Store[IO], pass: Option[Password]) =
    OShare[IO](store, cfg).use { share =>
      share
        .loadFile(ShareId.publish(publishId), fileId, pass, ByteRange.All)
        .value
    }

  test("checkFilePublish serves a file while views are below maxViews") {
    withStore { store =>
      for {
        _ <- insertShare(store, maxViews = 2, password = None)
        _ <- addView(store) // views = 1, still below 2
        res <- store.transact(Queries.checkFilePublish(publishId, fileId))
        _ <- IO(assertEquals(res, Some(None)))
      } yield ()
    }
  }

  test("checkFilePublish blocks a file once views reach maxViews") {
    withStore { store =>
      for {
        _ <- insertShare(store, maxViews = 2, password = None)
        _ <- addView(store) *> addView(store) // views = 2 == maxViews
        res <- store.transact(Queries.checkFilePublish(publishId, fileId))
        _ <- IO(assertEquals(res, None))
      } yield ()
    }
  }

  test("loadFile closes the file download once maxViews is reached") {
    withStore { store =>
      for {
        _ <- insertShare(store, maxViews = 1, password = None)
        _ <- addView(store) // views = 1 == maxViews, share is now closed
        res <- loadFile(store, None)
        _ <- IO(assertEquals(res, None)) // OptionT.none -> 404, not a password error
      } yield ()
    }
  }

  test("loadFile rejects a wrong password with a mismatch while still open") {
    withStore { store =>
      for {
        _ <- insertShare(store, maxViews = 2, password = Some(Password("secret")))
        res <- loadFile(store, Some(Password("wrong")))
        _ <- IO(assert(res.contains(ShareResult.PasswordMismatch)))
      } yield ()
    }
  }

  test("loadFile does not leak a password oracle after maxViews is reached") {
    withStore { store =>
      for {
        _ <- insertShare(store, maxViews = 1, password = Some(Password("secret")))
        _ <- addView(store) // exhaust the single view
        // a closed, password-protected share must answer the same (404/None)
        // whether the password is right, wrong, or missing
        wrong <- loadFile(store, Some(Password("wrong")))
        right <- loadFile(store, Some(Password("secret")))
        missing <- loadFile(store, None)
        _ <- IO(assertEquals(wrong, None))
        _ <- IO(assertEquals(right, None))
        _ <- IO(assertEquals(missing, None))
      } yield ()
    }
  }
}
