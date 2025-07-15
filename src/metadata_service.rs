use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use fuser::FileType;
use sqlx::{
    Arguments, PgPool, Postgres, QueryBuilder, Row,
    postgres::{PgArguments, PgRow, types::PgLTree as Ltree},
};
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct Inode {
    pub ino: i64,
    pub parent_ino: i64,
    pub name: String,
    pub path_ltree: Ltree,
    pub is_dir: bool,
    pub size: i64,
    pub perm: i16,
    pub nlink: i32,
    pub uid: i32,
    pub gid: i32,
    pub atime: OffsetDateTime,
    pub mtime: OffsetDateTime,
    pub ctime: OffsetDateTime,
    pub crtime: OffsetDateTime,
    pub etag: Option<String>,
    pub upload_id: Option<String>,
}

impl Inode {
    pub fn to_file_attr(&self) -> fuser::FileAttr {
        fuser::FileAttr {
            ino: self.ino as u64,
            size: self.size as u64,
            blocks: (self.size as u64 + 511) / 512,
            atime: self.atime.into(),
            mtime: self.mtime.into(),
            ctime: self.ctime.into(),
            crtime: self.crtime.into(),
            kind: if self.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: self.perm as u16,
            nlink: self.nlink as u32,
            uid: self.uid as u32,
            gid: self.gid as u32,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    pub fn from_row(row: &PgRow) -> Result<Self> {
        Ok(Self {
            ino: row.try_get("ino")?,
            parent_ino: row.try_get("parent_ino")?,
            name: row.try_get("name")?,
            path_ltree: row.try_get("path_ltree")?,
            is_dir: row.try_get("is_dir")?,
            size: row.try_get("size")?,
            perm: row.try_get("perm")?,
            nlink: row.try_get("nlink")?,
            uid: row.try_get("uid")?,
            gid: row.try_get("gid")?,
            atime: row.try_get("atime")?,
            mtime: row.try_get("mtime")?,
            ctime: row.try_get("ctime")?,
            crtime: row.try_get("crtime")?,
            etag: row.try_get("etag")?,
            upload_id: row.try_get("upload_id")?,
        })
    }
}

pub struct DirEntry {
    pub ino: u64,
    pub name: String,
    pub kind: FileType,
}

// --- Metadata Service ---

#[derive(Clone)]
pub struct MetadataService {
    pub pool: PgPool,
    inodes_table_fqn: String,
}

fn sanitize_identifier(name: &str) -> Result<String> {
    if name.is_empty()
        || name
            .chars()
            .any(|c| !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '_' && c != '-')
    {
        bail!(
            "Invalid bucket name for schema: must be non-empty and contain only lowercase letters, numbers, and underscores."
        );
    }
    Ok(name.to_string())
}

impl MetadataService {
    pub async fn new(db_url: &str, bucket_name: &str) -> Result<Self> {
        let pool = PgPool::connect(db_url)
            .await
            .context("Failed to create PostgreSQL connection pool")?;
        let schema_name = sanitize_identifier(bucket_name)?;
        Ok(Self {
            pool,
            inodes_table_fqn: format!("\"{}\".inodes", schema_name),
        })
    }

    pub async fn initialize_schema(&self, mount_uid: u32, mount_gid: u32) -> Result<()> {
        let schema_name = self.inodes_table_fqn.split('.').next().unwrap();
        let mut tx = self.pool.begin().await?;

        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name))
            .execute(&mut *tx)
            .await?;

        sqlx::query("CREATE EXTENSION IF NOT EXISTS ltree")
            .execute(&mut *tx)
            .await?;

        let create_table_sql = format!(
            r#"CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY, parent_ino BIGINT, name VARCHAR(255) NOT NULL, path_ltree LTREE NOT NULL,
                ino BIGINT UNIQUE NOT NULL, is_dir BOOLEAN NOT NULL, size BIGINT NOT NULL,
                perm SMALLINT NOT NULL, nlink INT NOT NULL, uid INT NOT NULL, gid INT NOT NULL,
                atime TIMESTAMPTZ NOT NULL, mtime TIMESTAMPTZ NOT NULL, ctime TIMESTAMPTZ NOT NULL, crtime TIMESTAMPTZ NOT NULL,
                etag TEXT, upload_id TEXT,
                CONSTRAINT uq_parent_name UNIQUE(parent_ino, name)
            )"#,
            self.inodes_table_fqn
        );
        sqlx::query(&create_table_sql).execute(&mut *tx).await?;

        // Add GIST index for ltree operations
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS path_ltree_idx ON {} USING GIST (path_ltree)",
            self.inodes_table_fqn
        ))
        .execute(&mut *tx)
        .await?;

        let check_root_sql = format!("SELECT 1 FROM {} WHERE ino = 1", self.inodes_table_fqn);
        if sqlx::query_scalar::<Postgres, i32>(&check_root_sql)
            .fetch_optional(&mut *tx)
            .await?
            .is_none()
        {
            let insert_root_sql = format!(
                "INSERT INTO {} (parent_ino, name, path_ltree, ino, is_dir, size, perm, nlink, uid, gid, atime, mtime, ctime, crtime)
                 VALUES (1, '/', '1', 1, TRUE, 4096, 16877, 2, $1, $2, NOW(), NOW(), NOW(), NOW())", // 0o755 is 493, with dir bit 16877
                self.inodes_table_fqn
            );
            sqlx::query(&insert_root_sql)
                .bind(mount_uid as i32)
                .bind(mount_gid as i32)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit()
            .await
            .context("Failed to initialize bucket schema")
    }

    pub async fn lookup(&self, parent_ino: u64, name: &str) -> Result<Option<Inode>> {
        let sql = format!(
            "SELECT * FROM {} WHERE parent_ino = $1 AND name = $2",
            self.inodes_table_fqn
        );

        tracing::debug!("lookup called for parent_ino={} name={}", parent_ino, name);
        sqlx::query(&sql)
            .bind(parent_ino as i64)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
            .map(|r| Inode::from_row(&r))
            .transpose()
    }

    pub async fn get_inode(&self, ino: u64) -> Result<Option<Inode>> {
        let sql = format!("SELECT * FROM {} WHERE ino = $1", self.inodes_table_fqn);
        sqlx::query(&sql)
            .bind(ino as i64)
            .fetch_optional(&self.pool)
            .await?
            .map(|r| Inode::from_row(&r))
            .transpose()
    }

    #[warn(clippy::too_many_arguments)]
    pub async fn update_inode(
        &self,
        ino: u64,
        size: Option<i64>,
        perm: Option<i16>,
        uid: Option<i32>,
        gid: Option<i32>,
        atime: Option<OffsetDateTime>,
        mtime: Option<OffsetDateTime>,
    ) -> Result<Inode> {
        // Bind `ino` first => $1
        let mut args = PgArguments::default();
        let _ = args.add(ino as i64);

        let mut set_clauses = Vec::new();

        if let Some(s) = size {
            let idx = args.len() + 1;
            set_clauses.push(format!("size = ${}", idx));
            let _ = args.add(s);
        }
        if let Some(p) = perm {
            let idx = args.len() + 1;
            set_clauses.push(format!("perm = ${}", idx));
            let _ = args.add(p);
        }
        if let Some(u) = uid {
            let idx = args.len() + 1;
            set_clauses.push(format!("uid = ${}", idx));
            let _ = args.add(u);
        }
        if let Some(g) = gid {
            let idx = args.len() + 1;
            set_clauses.push(format!("gid = ${}", idx));
            let _ = args.add(g);
        }
        if let Some(a) = atime {
            let idx = args.len() + 1;
            set_clauses.push(format!("atime = ${}", idx));
            let _ = args.add(a);
        }
        if let Some(m) = mtime {
            let idx = args.len() + 1;
            // use same placeholder for both mtime and ctime
            set_clauses.push(format!("mtime = ${}", idx));
            set_clauses.push(format!("ctime = ${}", idx));
            let _ = args.add(m);
        }

        // No changes requested? Just re-fetch the row.
        if set_clauses.is_empty() {
            return self.get_inode(ino).await?.context("Inode not found");
        }

        let sql = format!(
            "UPDATE {} SET {} WHERE ino = $1 RETURNING *",
            self.inodes_table_fqn,
            set_clauses.join(", ")
        );
        let updated_row = sqlx::query_with(&sql, args).fetch_one(&self.pool).await?;
        Inode::from_row(&updated_row)
    }

    pub async fn list_directory(&self, parent_ino: u64) -> Result<Vec<DirEntry>> {
        let sql = format!(
            "SELECT ino, name, is_dir FROM {} WHERE parent_ino = $1 AND ino != $1",
            self.inodes_table_fqn
        );
        sqlx::query(&sql)
            .bind(parent_ino as i64)
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(|row| {
                Ok(DirEntry {
                    ino: row.get::<i64, _>("ino") as u64,
                    name: row.get("name"),
                    kind: if row.get("is_dir") {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    },
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    #[warn(clippy::too_many_arguments)]
    pub async fn create_inode(
        &self,
        parent_ino: u64,
        new_ino: u64,
        name: &str,
        mode: u32,
        kind: FileType,
        uid: u32,
        gid: u32,
    ) -> Result<Inode> {
        let mut tx = self.pool.begin().await?;

        let parent_path: Ltree = sqlx::query_scalar(&format!(
            "SELECT path_ltree FROM {} WHERE ino = $1 FOR UPDATE",
            self.inodes_table_fqn
        ))
        .bind(parent_ino as i64)
        .fetch_one(&mut *tx)
        .await?;

        let new_path: Ltree = format!("{}.{}", parent_path, new_ino).parse()?;

        let is_dir = kind == FileType::Directory;
        let nlink = if is_dir { 2 } else { 1 };
        let now = OffsetDateTime::now_utc();

        let insert_sql = format!(
            "INSERT INTO {} (parent_ino, name, path_ltree, ino, is_dir, size, perm, nlink, uid, gid, atime, mtime, ctime, crtime)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $11, $11) RETURNING *", self.inodes_table_fqn
        );
        let new_row = sqlx::query(&insert_sql)
            .bind(parent_ino as i64)
            .bind(name)
            .bind(new_path)
            .bind(new_ino as i64)
            .bind(is_dir)
            .bind(0i64)
            .bind(mode as i16)
            .bind(nlink)
            .bind(uid as i32)
            .bind(gid as i32)
            .bind(now)
            .fetch_one(&mut *tx)
            .await?;

        if is_dir {
            sqlx::query(&format!(
                "UPDATE {} SET nlink = nlink + 1 WHERE ino = $1",
                self.inodes_table_fqn
            ))
            .bind(parent_ino as i64)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Inode::from_row(&new_row)
    }

    pub async fn create_inodes_batch(
        &self,
        parent_ino: u64,
        new_inodes_data: Vec<(u64, String, u32, FileType, u32, u32)>,
    ) -> Result<()> {
        if new_inodes_data.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        let parent_path: Ltree = sqlx::query_scalar(&format!(
            "SELECT path_ltree FROM {} WHERE ino = $1 FOR UPDATE",
            self.inodes_table_fqn
        ))
        .bind(parent_ino as i64)
        .fetch_one(&mut *tx)
        .await?;

        // Use QueryBuilder to create a single, efficient multi-row INSERT statement.
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(format!(
            "INSERT INTO {} (parent_ino, name, path_ltree, ino, is_dir, size, perm, nlink, uid, gid, atime, mtime, ctime, crtime)",
            self.inodes_table_fqn
        ));

        query_builder.push_values(
            new_inodes_data.iter(),
            |mut b, (new_ino, name, mode, kind, uid, gid)| {
                let is_dir = *kind == FileType::Directory;
                let nlink = if is_dir { 2 } else { 1 };

                let new_path: Ltree = format!("{}.{}", parent_path, new_ino).parse().unwrap();

                b.push_bind(parent_ino as i64)
                    .push_bind(name)
                    .push_bind(new_path)
                    .push_bind(*new_ino as i64)
                    .push_bind(is_dir)
                    .push_bind(0i64) // size
                    .push_bind(*mode as i16)
                    .push_bind(nlink)
                    .push_bind(*uid as i32)
                    .push_bind(*gid as i32)
                    .push_bind(OffsetDateTime::now_utc()) // atime
                    .push_bind(OffsetDateTime::now_utc()) // mtime
                    .push_bind(OffsetDateTime::now_utc()) // ctime
                    .push_bind(OffsetDateTime::now_utc()); // crtime
            },
        );

        let query = query_builder.build();
        query.execute(&mut *tx).await?;

        // Update parent nlink count based on the number of new directories created.
        let dir_nlink_increase = new_inodes_data
            .iter()
            .filter(|(_, _, _, kind, _, _)| *kind == FileType::Directory)
            .count();
        if dir_nlink_increase > 0 {
            sqlx::query(&format!(
                "UPDATE {} SET nlink = nlink + $1 WHERE ino = $2",
                self.inodes_table_fqn
            ))
            .bind(dir_nlink_increase as i32)
            .bind(parent_ino as i64)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn remove_inode(&self, parent_ino: u64, name: &str) -> Result<Inode> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!(
            "SELECT 1 FROM {} WHERE ino = $1 FOR UPDATE",
            self.inodes_table_fqn
        ))
        .bind(parent_ino as i64)
        .execute(&mut *tx)
        .await?;

        let delete_sql = format!(
            "DELETE FROM {} WHERE parent_ino = $1 AND name = $2 RETURNING *",
            self.inodes_table_fqn
        );
        let deleted_inode = match sqlx::query(&delete_sql)
            .bind(parent_ino as i64)
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?
        {
            Some(row) => Inode::from_row(&row)?,
            None => bail!("Cannot delete: Inode '{}/{}' not found", parent_ino, name),
        };

        if deleted_inode.is_dir {
            let child_count: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {} WHERE parent_ino = $1",
                self.inodes_table_fqn
            ))
            .bind(deleted_inode.ino)
            .fetch_one(&mut *tx)
            .await?;
            if child_count > 0 {
                bail!("Directory not empty");
            }
            sqlx::query(&format!(
                "UPDATE {} SET nlink = nlink - 1 WHERE ino = $1",
                self.inodes_table_fqn
            ))
            .bind(parent_ino as i64)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(deleted_inode)
    }

    pub async fn rename_inode_with_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        ino: u64,
        new_parent_ino: u64,
        new_name: &str,
    ) -> Result<()> {
        // Lock rows and get old inode info
        let inode_to_move: Inode = sqlx::query(&format!(
            "SELECT * FROM {} WHERE ino = $1 FOR UPDATE",
            self.inodes_table_fqn
        ))
        .bind(ino as i64)
        .fetch_one(&mut **tx) // Note: using the transaction
        .await
        .map(|r| Inode::from_row(&r).unwrap())?;

        let old_parent_ino = inode_to_move.parent_ino;

        // Get new parent's path
        let new_parent_path: Ltree = sqlx::query_scalar(&format!(
            "SELECT path_ltree FROM {} WHERE ino = $1 FOR UPDATE",
            self.inodes_table_fqn
        ))
        .bind(new_parent_ino as i64)
        .fetch_one(&mut **tx) // Note: using the transaction
        .await?;

        // Calculate new path_ltree for the moving inode
        let new_path: Ltree = format!("{}.{}", new_parent_path, ino).parse()?;

        // Update the inode itself
        sqlx::query(&format!(
            "UPDATE {} SET parent_ino = $1, name = $2, path_ltree = $3, mtime = NOW(), ctime = NOW() WHERE ino = $4",
            self.inodes_table_fqn
        ))
        .bind(new_parent_ino as i64)
        .bind(new_name)
        .bind(&new_path)
        .bind(ino as i64)
        .execute(&mut **tx) // Note: using the transaction
        .await?;

        // If it was a directory, update paths of all its descendants
        if inode_to_move.is_dir {
            let old_path_str = inode_to_move.path_ltree.to_string();
            let new_path_str = new_path.to_string();

            sqlx::query(&format!(
                "UPDATE {} SET path_ltree = text2ltree(replace(ltree2text(path_ltree), $1, $2)) WHERE path_ltree <@ $3 AND ino != $4",
                self.inodes_table_fqn
            ))
            .bind(&old_path_str)
            .bind(&new_path_str)
            .bind(&inode_to_move.path_ltree)
            .bind(ino as i64)
            .execute(&mut **tx) // Note: using the transaction
            .await?;
        }

        // Update nlink counts if it was a directory and the parent changed
        if old_parent_ino != new_parent_ino as i64 && inode_to_move.is_dir {
            // Decrement old parent's nlink
            sqlx::query(&format!(
                "UPDATE {} SET nlink = nlink - 1 WHERE ino = $1",
                self.inodes_table_fqn
            ))
            .bind(old_parent_ino)
            .execute(&mut **tx) // Note: using the transaction
            .await?;

            // Increment new parent's nlink
            sqlx::query(&format!(
                "UPDATE {} SET nlink = nlink + 1 WHERE ino = $1",
                self.inodes_table_fqn
            ))
            .bind(new_parent_ino as i64)
            .execute(&mut **tx) // Note: using the transaction
            .await?;
        }

        Ok(())
    }

    // The original function now becomes a convenience wrapper that creates its own transaction.
    pub async fn rename_inode(&self, ino: u64, new_parent_ino: u64, new_name: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.rename_inode_with_tx(&mut tx, ino, new_parent_ino, new_name)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_full_path(&self, ino: u64) -> Result<PathBuf> {
        let mut path_segments: Vec<String> = Vec::new();
        let mut current_ino = ino;

        // Loop until we hit the root directory.
        while current_ino != 1 {
            let inode = self
                .get_inode(current_ino)
                .await?
                .context(format!("Failed to find inode {} in hierarchy", current_ino))?;

            // The root inode itself shouldn't be part of the path segments.
            if inode.ino == 1 {
                break;
            }

            path_segments.push(inode.name);
            current_ino = inode.parent_ino as u64;
        }

        // The segments are in reverse order (e.g., ["file.txt", "robot", "hanna"]),
        // so we reverse them and build the final path.
        let path: PathBuf = path_segments.into_iter().rev().collect();
        Ok(path)
    }
}
