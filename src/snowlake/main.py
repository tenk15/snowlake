from argparse import ArgumentParser, Namespace
from dataclasses import asdict, dataclass
from enum import Enum
from json import dumps, loads
from pathlib import Path
from re import compile
from shutil import copy
from subprocess import check_output, run
from sys import exit, stderr, stdin, stdout
from tempfile import TemporaryDirectory
from tomllib import load
from typing import Any, ClassVar, Literal, Protocol, Self

from dotenv import find_dotenv
from pydantic import BaseModel, Field
from snowflake.core import Root
from snowflake.core.exceptions import NotFoundError
from snowflake.core.schema import Schema
from snowflake.core.stage import Stage, StageDirectoryTable, StageEncryption
from snowflake.core.table import Table, TableColumn
from snowflake.snowpark import Row, Session
from snowflake.snowpark.functions import col, current_timestamp, lit


class IsDataclass(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]


# command line and toml  parser logic
# ===================================


class Subcommands(Enum):
    init = "init"
    transfer = "transfer"
    checkout = "checkout"


class SnowlakeURI(BaseModel):
    snowlake_uri: str = Field(alias="snowlake-uri")

    def __str__(self) -> str:
        return self.snowlake_uri


type Remote = str


class SnowlakeOptions(BaseModel):
    schema_: str = Field("snowlake", alias="schema")
    catalog: str = Field("catalog", alias="catalog")
    obj: str = Field("obj", alias="obj")
    head: str = Field("HEAD", alias="HEAD")
    reflog: str = Field("REFLOG", alias="reflog")
    remotes: dict[Remote, SnowlakeURI] = Field(alias="remote")

    @classmethod
    def new(cls) -> Self:
        path = Path(find_dotenv(filename="snowlake.toml", usecwd=True))
        if path.is_dir():
            raise FileNotFoundError("Couldn't find snowlake.toml")
        return cls.model_validate(load(open(path, "rb")))


@dataclass
class TopLevelParser:
    _parser: ArgumentParser

    @classmethod
    def new(cls) -> Self:
        parser = ArgumentParser(prog="snowlake")
        subparsers = parser.add_subparsers(dest="command")
        _ = subparsers.add_parser(name="init")
        _ = subparsers.add_parser(name="transfer")
        checkout_parser = subparsers.add_parser(name="checkout")
        checkout_parser.add_argument("remote")
        checkout_parser.add_argument("branch")
        checkout_parser.add_argument("database")
        return cls(_parser=parser)

    def parse_args(self) -> Namespace:
        return self._parser.parse_args()


class CheckoutArguments(BaseModel):
    remote: str
    branch: str
    database: str


def main():
    parser = TopLevelParser.new()
    args = vars(parser.parse_args())
    match Subcommands(args["command"]):
        case Subcommands.init:
            init(SnowlakeOptions.new())
        case Subcommands.transfer:
            try:
                opts = SnowlakeOptions.new()
                transfer(opts)
            except Exception as exc:
                exception_handler(exc)
        case Subcommands.checkout:
            args = CheckoutArguments.model_validate(args)
            checkout(args, SnowlakeOptions.new())


# lfs protocol model
# ==================


class LFSEvent(Enum):
    init = "init"
    upload = "upload"
    download = "download"
    terminate = "terminate"


class LFSOperation(Enum):
    upload = "upload"
    download = "download"


class LFSInitMessage(BaseModel):
    event: Literal["init"]
    operation: LFSOperation
    remote: str
    concurrent: bool
    concurrenttransfers: int


class LFSUploadMessage(BaseModel):
    event: Literal["upload"]
    oid: str
    size: int
    path: Path
    # We dont care about this
    action: Any


class LFSDownloadMessage(BaseModel):
    event: Literal["download"]
    oid: str
    size: int
    # We dont care about this
    action: Any


@dataclass
class LFSProgressMessage:
    event: Literal["progress"]
    oid = str
    bytesSoFar: int
    bytesSinceLast: int


@dataclass
class LFSError:
    code: int
    message: str


@dataclass
class LFSErrorMessage:
    error: LFSError | None


@dataclass
class LFSCompleteMessage:
    event: Literal["complete"]
    oid: str
    error: LFSError | None
    path: str | None


class TransferAgentException(Exception):
    def __init__(self, *args, message: LFSCompleteMessage, **kwargs):
        self.message = message
        super().__init__(*args, **kwargs)


class InitTransferAgentException(Exception):
    pass


# parsing endpoint uri
# ===================


ENDPOINT_REGEX = compile(r"^snowflake://(?P<connection>[^/]+)/(?P<database>[^/]+)$")


def parse_endpoint(uri: SnowlakeURI) -> tuple[str, str]:
    match = ENDPOINT_REGEX.match(str(uri))
    if (
        not match
        or not (connection_name := str(match.group("connection")))
        or not (database := str(match.group("database")))
    ):
        raise InitTransferAgentException(f"Connection endpoint malformed. Got {uri!s}")
    return connection_name, database


# database objects created by init
# ================================


CATALOG_SCHEMA: list[TableColumn] = [
    TableColumn(name="OID", datatype="VARCHAR(16777216)")
]

HEAD_SCHEMA: list[TableColumn] = [
    TableColumn(name="FILE", datatype="VARCHAR(16777216)"),
    TableColumn(name="OID", datatype="VARCHAR(16777216)"),
    TableColumn(name="DATABASE", datatype="VARCHAR(16777216)"),
    TableColumn(name="SCHEMA", datatype="VARCHAR(16777216)"),
    TableColumn(name="STAGE", datatype="VARCHAR(16777216)"),
]

REFLOG_SCHEMA: list[TableColumn] = [
    TableColumn(
        name="SEQ",
        datatype="NUMBER(38,0)",
        autoincrement=True,
        autoincrement_start=1,
        autoincrement_increment=1,
    ),
    TableColumn(name="SHA", datatype="VARCHAR(16777216)"),
    TableColumn(name="MSG", datatype="VARCHAR(16777216)"),
    TableColumn(name="TMSTMP", datatype="TIMESTAMP_LTZ(9)"),
]


# init cli
# ========

type Connection = str


def init(opts: SnowlakeOptions) -> None:
    sessions: dict[Connection, Session] = {}
    for uri in opts.remotes.values():
        connection_name, database = parse_endpoint(uri)
        if not connection_name in sessions:
            sessions[connection_name] = Session.builder.config(
                "connection_name", connection_name
            ).create()
        session = sessions[connection_name]
        catalog = session.catalog
        root = Root(session)
        if not catalog.database_exists(database):
            raise RuntimeError(
                "The specified snowflake database does not exists or is not authorized."
            )
        database_ressource = root.databases[database]
        if not catalog.schema_exists(schema=opts.schema_, database=database):
            database_ressource.schemas.create(Schema(name=opts.schema_))
        schema_ressource = database_ressource.schemas[opts.schema_]
        if not catalog.table_exists(
            table=opts.catalog, database=database, schema=opts.schema_
        ):
            schema_ressource.tables.create(
                Table(
                    name=opts.catalog,
                    columns=CATALOG_SCHEMA,
                )
            )
        # Sadly catalog doesnt expose stages (yet?), so we need a try/catch
        try:
            schema_ressource.stages[opts.obj].fetch()
        except NotFoundError:
            schema_ressource.stages.create(
                Stage(
                    name=opts.obj,
                    encryption=StageEncryption(type="SNOWFLAKE_FULL"),
                    directory_table=StageDirectoryTable(enable=True, auto_refresh=True),
                )
            )
        run(["git", "lfs", "install"], check=True)
        run(
            ["git", "config", "lfs.customtransfer.snowlake.path", "snowlake"],
            check=True,
        )
        run(
            ["git", "config", "lfs.customtransfer.snowlake.args", "transfer"],
            check=True,
        )
        run(
            ["git", "config", "lfs.customtransfer.snowlake.concurrent", "false"],
            check=True,
        )
        run(["git", "config", "lfs.repositoryformatversion", "0"], check=True)
        run(["git", "config", "lfs.standalonetransferagent", "snowlake"], check=True)


# custom transfer protocol
# =======================


def transfer(opts: SnowlakeOptions):
    session: Session | None = None
    database: str | None = None
    for message in stdin:
        message_dict = loads(message)
        match LFSEvent(message_dict["event"]):
            case LFSEvent.init:
                session, database = handle_lfs_init(
                    LFSInitMessage.model_validate(message_dict), opts
                )
                send_message(LFSErrorMessage(error=None))

            case LFSEvent.upload:
                assert session is not None
                assert database is not None
                handle_lfs_upload(
                    LFSUploadMessage.model_validate(message_dict),
                    opts,
                    session,
                    database,
                )

            case LFSEvent.download:
                assert session is not None
                assert database is not None
                handle_lfs_download(
                    LFSDownloadMessage.model_validate(message_dict),
                    opts,
                    session,
                    database,
                )

            case LFSEvent.terminate:
                return


def handle_lfs_init(
    message: LFSInitMessage, opts: SnowlakeOptions
) -> tuple[Session, str]:
    connection_name, database = parse_endpoint(opts.remotes[message.remote])
    session = Session.builder.config("connection_name", connection_name).create()
    catalog = session.catalog
    root = Root(session)
    if not catalog.database_exists(database):
        raise InitTransferAgentException(
            "The specified snowflake database does not exists or is not authorized."
        )
    if not catalog.schema_exists(schema=opts.schema_, database=database):
        raise InitTransferAgentException(
            "The specified snowflake schema does not exists or is not authorized."
        )
    if not catalog.table_exists(
        table=opts.catalog,
        database=database,
        schema=opts.schema_,
    ):
        raise InitTransferAgentException(
            "The catalog table does not exists or is not authorized."
        )
    if (
        not catalog.get_table(
            opts.catalog,
            database=database,
            schema=opts.schema_,
        ).columns
        == CATALOG_SCHEMA
    ):
        raise InitTransferAgentException(
            "The catalog tables schema does not agree with the protocol."
        )
    # Sadly catalog doesnt expose stages yet, so we need a try/catch
    try:
        root.databases[database].schemas[opts.schema_].stages[opts.obj].fetch()
    except NotFoundError as exc:
        raise InitTransferAgentException(
            "The object store stage does not exists or is not authorized."
        ) from exc
    return session, database


def send_message(obj: IsDataclass):
    stdout.write(dumps(asdict(obj)) + "\n")
    stdout.flush()


def exception_handler(exc: Exception):
    try:
        raise exc
    except TransferAgentException as exc_:
        send_message(exc_.message)
    except InitTransferAgentException:
        send_message(
            LFSErrorMessage(
                LFSError(1, f"Transfer Agent Exception in initialization:{exc!s}")
            )
        )
        stderr.write(f"Transfer Agent Exception in initialization:{exc!s}")
        stderr.flush()
        exit(1)
    except Exception:
        send_message(
            LFSErrorMessage(
                LFSError(2, f"Unexspected fatal exception: {type(exc)!s} {exc!s}")
            )
        )
        stderr.write(f"Unexspected fatal exception: {type(exc)!s} {exc!s}")
        stderr.flush()
        exit(2)


def handle_lfs_upload(
    message: LFSUploadMessage, opts: SnowlakeOptions, session: Session, database: str
):
    try:
        catalog = [database, opts.schema_, opts.catalog]
        if len(
            session.table(catalog)
            .select(col("oid"))
            .where(col("oid") == message.oid)
            .collect()
        ):
            send_message(
                LFSCompleteMessage(
                    event="complete", oid=message.oid, error=None, path=None
                )
            )
            return
        with TemporaryDirectory(dir=".git") as dir:
            copy(message.path, Path(dir) / message.oid)
            session.file.put(
                str(Path(dir) / message.oid),
                f"@{database}.{opts.schema_}.{opts.obj}",
                auto_compress=False,
            )
        row = session.create_dataframe([Row(message.oid)])
        row.write.save_as_table(catalog, mode="append")
        send_message(
            LFSCompleteMessage(event="complete", oid=message.oid, error=None, path=None)
        )

    except Exception as exc:
        raise TransferAgentException(
            message=LFSCompleteMessage(
                "complete",
                message.oid,
                error=LFSError(
                    code=2, message=f"Unexspected exception: {type(exc)!s} {exc!s}"
                ),
                path=None,
            )
        ) from exc


def handle_lfs_download(
    message: LFSDownloadMessage, opts: SnowlakeOptions, session: Session, database: str
):
    try:
        _ = session.file.get(
            f"@{database}.{opts.schema_}.{opts.obj}/{message.oid}",
            target_directory=str(Path(".git")),
        )
        send_message(
            LFSCompleteMessage(
                event="complete",
                oid=message.oid,
                error=None,
                path=str(Path(".git") / Path(message.oid)),
            )
        )
    except Exception as exc:
        raise TransferAgentException(
            message=LFSCompleteMessage(
                event="complete",
                oid=message.oid,
                error=LFSError(
                    code=2, message=f"Unexspected exception: {type(exc)!s} {exc!s}"
                ),
                path=None,
            )
        ) from exc


# checkout cli
# ============


def smudge(session: Session, head_ref: str, path: str) -> str:
    db = session.get_current_database()
    head_ref_regx = compile(r"(?P<schema>[^\.]+)\.(?P<table>[^\.]+)$")
    match = head_ref_regx.match(head_ref)
    if (
        not match
        or not (schema := str(match.group("schema")))
        or not (table := str(match.group("table")))
    ):
        raise RuntimeError(
            f"HEAD Table reference malformed. Got {head_ref}. Expected <SCHEMA>.<TABLE>."
        )
    if not session.catalog.table_exists(table, database=db, schema=schema):
        raise RuntimeError(f"HEAD Table {db}.{schema}.{table} does not exists.")
    assert db is not None, "This function should be only ever run as owner"
    df = session.table([db, schema, table])
    row = (
        df.select(col("OID"), col("DATABASE"), col("SCHEMA"), col("STAGE"))
        .where(lit(path) == col("FILE"))
        .collect()
        .pop()
        .as_dict()
    )
    return f"@{row['DATABASE']}.{row['SCHEMA']}.{row['STAGE']}/{row['OID']}"


smudge.__module__ = "__main__"


def checkout(args: CheckoutArguments, opts: SnowlakeOptions):
    try:
        connection_name, root_database = parse_endpoint(opts.remotes[args.remote])
    except KeyError as e:
        raise RuntimeError(f"{args.remote} is no configured remote.") from e
    session = Session.builder.config("connection_name", connection_name).create()
    if not session.catalog.database_exists(args.database):
        raise RuntimeError(
            f"Database {args.database} does not exist or not authorized."
        )
    if not session.catalog.schema_exists(opts.schema_, database=args.database):
        Root(session).databases[args.database].schemas.create(Schema(opts.schema_))
    _ = run(["git", "fetch"], check=True)
    commit, message = (
        check_output(
            ["git", "show", "--format=%H %B", "--no-patch", f"{args.branch}"], text=True
        )
        .strip()
        .split()
    )
    if not check_output(
        [
            "git",
            "branch",
            "-r",
            "--contains",
            f"{commit}",
            f"{args.remote}/*",
        ],
        text=True,
    ):
        raise RuntimeError(f"Commit {commit} has not been pushed to the remote yet.")

    tree = check_output(
        ["git", "lfs", "ls-files", "--long", f"{args.branch}"], text=True
    ).strip()
    rows: list[dict[str, str]] = []
    for line in tree.splitlines():
        oid, _, file = line.split()
        rows.append(
            {
                "FILE": file,
                "OID": oid,
                "DATABASE": root_database,
                "SCHEMA": opts.schema_,
                "STAGE": opts.obj,
            }
        )
    df = session.create_dataframe(rows)
    df.write.save_as_table([args.database, opts.schema_, opts.head], mode="overwrite")
    if not session.catalog.table_exists(
        opts.reflog, database=args.database, schema=opts.schema_
    ):
        Root(session).databases[args.database].schemas[opts.schema_].tables.create(
            Table(
                name=opts.reflog,
                columns=REFLOG_SCHEMA,
            )
        )
    if (
        not session.catalog.get_table(
            opts.reflog,
            database=args.database,
            schema=opts.schema_,
        ).columns
        == REFLOG_SCHEMA
    ):
        raise RuntimeError(
            "The catalog tables schema does not agree with the protocol."
        )
    df = session.create_dataframe([{"SHA": commit, "MSG": message}])
    df = df.with_column("TMSTMP", current_timestamp())
    df.write.save_as_table(
        [args.database, opts.schema_, opts.reflog], mode="append", column_order="name"
    )
    session.sproc.register(
        smudge,
        name=f"{args.database}.{opts.schema_}.SMUDGE",
        replace=True,
        is_permanent=True,
        stage_location=f"@{args.database}.{opts.schema_}.{opts.obj}",
        packages=["snowflake-snowpark-python", "snowflake-core"],
    )
