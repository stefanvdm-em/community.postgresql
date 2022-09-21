#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2022, Stefan van der Merwe (@stefanvdm-em)
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r"""
---
module: postgresql_funcs
short_description: Add, update, or remove PostgreSQL functions
description:
- Allows to create, drop or modify a PostgreSQL function
options:
  function:
    description:
    - Function name.
    required: true
    aliases:
    - name
    type: str
  state:
    description:
    - The function state I(state=absent) is mutually exclusive with I(language), I(return_type), I(source), I(volatile),
      I(strict), I(security_definer), and I(owner).
    type: str
    default: present
    choices: [ absent, present ]
  owner:
    description:
    - Set a function owner.
    type: str
  language:
    description:
    - The language the function is written in.
    type: str
  return_type:
    description:
    - The return type of the function.
    type: str
  source:
    description:
    - The source code of the function.
    type: str
  arguments:
    description:
    - A list of arguments for the function.
    type: list
    elements: str
  volatile:
    description:
    - Function volatilty.
    type: str
    choices: [ immutable, stable, volatile ]
  is_strict:
    description:
    - Whether the function is strict.
    aliases:
    - strict
    type: bool
    default: false
  is_security_definer:
    description:
    - Whether the function is a security definer.
    aliases:
    - security_definer
    type: bool
    default: false
  rename:
    description:
    - The new name of the function. Mutually exclusive with I(state=absent), I(language), I(return_type), I(source),
      I(volatile), I(strict), I(security_definer), and I(owner).
    type: str
  db:
    description:
    - Name of database to connect and where the function will be created.
    type: str
    aliases:
    - login_db
  session_role:
    description:
    - Switch to session_role after connecting.
      The specified session_role must be a role that the current login_user is a member of.
    - Permissions checking for SQL commands is carried out as though
      the session_role were the one that had logged in originally.
    type: str
  cascade:
    description:
    - Automatically drop objects that depend on the function (such as views).
      Used with I(state=absent) only.
    type: bool
    default: false
  trust_input:
    description:
    - If C(false), check whether values of parameters are potentially dangerous.
    - It makes sense to use C(false) only when SQL injections are possible.
    type: bool
    default: true
notes:
- Supports C(check_mode).
- If you do not pass db parameter, functions will be created in the database
  named postgres.
seealso:
- module: community.postgresql.postgresql_sequence
- module: community.postgresql.postgresql_idx
- module: community.postgresql.postgresql_info
- module: community.postgresql.postgresql_owner
- module: community.postgresql.postgresql_privs
- module: community.postgresql.postgresql_copy
- name: CREATE FUNCTION reference
  description: Complete reference of the CREATE FUNCTION command documentation.
  link: https://www.postgresql.org/docs/current/sql-createfunction.html
- name: ALTER FUNCTION reference
  description: Complete reference of the ALTER FUNCTION command documentation.
  link: https://www.postgresql.org/docs/current/sql-alterfunction.html
- name: DROP FUNCTION reference
  description: Complete reference of the DROP FUNCTION command documentation.
  link: https://www.postgresql.org/docs/current/sql-dropfunction.html
- name: PostgreSQL data types
  description: Complete reference of the PostgreSQL data types documentation.
  link: https://www.postgresql.org/docs/current/datatype.html
author:
- Stefan van der Merwe (@stefanvdm-em)
extends_documentation_fragment:
- community.postgresql.postgres

"""

EXAMPLES = r"""
- name: Create a function
  community.postgresql.postgresql_funcs:
    function: myfunc
    language: plpgsql
    return_type: integer
    source: "BEGIN RETURN 1; END;"
    arguments:
      - "a integer"
      - "b integer"
    db: mydb

"""

RETURN = r"""
function:
  description: Name of the function.
  returned: always
  type: str
  sample: 'myfunc'
state:
  description: Function state.
  returned: always
  type: str
  sample: 'present'
owner:
  description: Function owner.
  returned: always
  type: str
  sample: 'postgres'
language:
  description: Function language.
  returned: always
  type: str
  sample: 'plpgsql'
return_type:
  description: Function return type.
  returned: always
  type: str
  sample: 'integer'
arguments:
  description: Function arguments.
  returned: always
  type: str
  sample: 'a integer, b integer'
volatile:
  description: Function volatility.
  returned: always
  type: str
  sample: 'v'
is_strict:
  description: Whether the function is strict.
  returned: always
  type: bool
  sample: false
is_security_definer:
  description: Whether the function is a security definer.
  returned: always
  type: bool
  sample: false
source:
  description: Function source code.
  returned: always
  type: str
  sample: 'BEGIN RETURN 1; END;'
"""

try:
    from psycopg2.extras import DictCursor
except ImportError:
    # psycopg2 is checked by connect_to_db()
    # from ansible.module_utils.postgres import missing_required_lib
    pass

from ansible.module_utils.basic import AnsibleModule
from ansible_collections.community.postgresql.plugins.module_utils.database import (
    check_input,
    pg_quote_identifier,
)
from ansible_collections.community.postgresql.plugins.module_utils.postgres import (
    connect_to_db,
    exec_sql,
    ensure_required_libs,
    get_conn_params,
    postgres_common_argument_spec,
)


# ===========================================
# PostgreSQL module specific support methods.
#


class Function(object):
    def __init__(self, name, module, cursor):
        self.name = name
        self.module = module
        self.cursor = cursor
        self.info = {
            "owner": "",
            "language": "",
            "arguments": "",
            "return_type": "",
            "volatile": "",
            "is_strict": "",
            "is_secdef": "",
            "source": "",
        }
        self.exists = False
        self.__exists_in_db()
        self.executed_queries = []

    def get_info(self):
        """Getter to refresh and get function info"""
        self.__exists_in_db()

    def __exists_in_db(self):
        """Check function exists and refresh info"""
        if "." in self.name:
            schema = self.name.split(".")[-2]
            fcnname = self.name.split(".")[-1]
        else:
            schema = "public"
            fcnname = self.name

        query = (
            "SELECT pa.rolname, pg_get_function_arguments(pp.oid), pl.lanname, "
            "pt.typname, pp.provolatile, pp.proisstrict, pp.prosecdef, pp.prosrc "
            "FROM pg_catalog.pg_proc AS pp "
            "INNER JOIN pg_catalog.pg_language AS pl ON pl.oid = pp.prolang "
            "INNER JOIN pg_catalog.pg_type AS pt ON pt.oid = pp.prorettype "
            "INNER JOIN pg_catalog.pg_authid AS pa ON pa.oid = pp.proowner "
            "WHERE prokind = 'f' "
            "AND proname = %(fcnname)s "
            "AND pronamespace = %(schema)s::regnamespace::oid"
        )
        res = exec_sql(
            self,
            query,
            query_params={"fcnname": fcnname, "schema": schema},
            add_to_executed=False,
        )
        if res:
            self.exists = True
            (
                owner,
                arguments,
                language,
                return_type,
                volatile,
                is_strict,
                is_secdef,
                source,
            ) = res[0]
            self.info = dict(
                owner=owner,
                arguments=arguments,
                language=language,
                return_type=return_type,
                volatile=volatile,
                is_strict=is_strict,
                is_secdef=is_secdef,
                source=source,
            )

            return True
        else:
            self.exists = False
            return False

    def create(
        self,
        language,
        return_type,
        source,
        arguments="",
        volatile="v",
        is_strict=False,
        is_secdef=False,
        owner="",
    ):
        name = pg_quote_identifier(self.name, "function")

        changed = False

        if self.exists:
            replace_function = False

            if self.info["return_type"] != return_type:
                self.module.fail_json(
                    msg=(
                        "Cannot change return type of existing function. "
                        "Please drop and recreate the function."
                    )
                )

            if owner and self.info["owner"] != owner:
                self.set_owner(owner)
                changed = True

            if self.info["language"] != language:
                replace_function = True

            if self.info["arguments"] != arguments:
                self.module.warn(
                    "Cannot change arguments of existing function. A new distinct function will be created. "
                    "See postgresql function overloading for more information."
                )
                replace_function = True

            if self.info["volatile"] != volatile:
                replace_function = True

            if self.info["is_strict"] != is_strict:
                replace_function = True

            if self.info["is_secdef"] != is_secdef:
                replace_function = True

            if self.info["source"] != source:
                replace_function = True

            if replace_function:
                if self.create_or_replace(
                    language=language,
                    return_type=return_type,
                    source=source,
                    arguments=arguments,
                    volatile=volatile,
                    is_strict=is_strict,
                    is_secdef=is_secdef,
                ):
                    changed = True

            if changed:
                return True
            return False

        if self.create_or_replace(
            language=language,
            return_type=return_type,
            source=source,
            arguments=arguments,
            volatile=volatile,
            is_strict=is_strict,
            is_secdef=is_secdef,
        ):
            changed = True

        if owner:
            self.set_owner(owner)

    def rename(self, newname):
        query = "ALTER FUNCTION %s RENAME TO %s" % (
            pg_quote_identifier(self.name, "function"),
            pg_quote_identifier(newname, "function"),
        )
        return exec_sql(self, query, return_bool=True)

    def set_owner(self, username):
        query = "ALTER FUNCTION %s OWNER TO %s" % (
            pg_quote_identifier(self.name, "function"),
            username,
        )
        return exec_sql(self, query, return_bool=True)

    def drop(self, cascade=False):
        if not self.exists:
            return False

        query = "DROP FUNCTION %s" % pg_quote_identifier(self.name, "function")
        if cascade:
            query += " CASCADE"
        return exec_sql(self, query, return_bool=True)

    def create_or_replace(
        self,
        language,
        return_type,
        source,
        arguments="",
        volatile="v",
        is_strict=False,
        is_secdef=False,
    ):

        query = "CREATE OR REPLACE FUNCTION %s" % (
            pg_quote_identifier(self.name, "function")
        )
        # arguments
        if arguments:
            query += "(%s)" % arguments
        else:
            query += "()"
        # return_type
        query += "RETURNS %s" % return_type
        # language
        query += "LANGUAGE %s" % language
        # volatile
        if volatile == "i":
            query += " IMMUTABLE"
        elif volatile == "s":
            query += " STABLE"
        elif volatile == "v":
            query += " VOLATILE"
        # is_strict
        if is_strict:
            query += " STRICT"
        # is_secdef
        if is_secdef:
            query += " SECURITY DEFINER"
        # source
        query += " AS $$%s$$" % source
        return exec_sql(self, query, return_bool=True, add_to_executed=True)


# ===========================================
# Module execution.
#


def main():
    argument_spec = postgres_common_argument_spec()
    argument_spec.update(
        function=dict(type="str", required=True, aliases=["name"]),
        state=dict(type="str", default="present", choices=["absent", "present"]),
        db=dict(type="str", default="", aliases=["login_db"]),
        owner=dict(type="str"),
        rename=dict(type="str"),
        language=dict(type="str"),
        return_type=dict(type="str"),
        source=dict(type="str"),
        arguments=dict(type="list", elements="str"),
        volatile=dict(type="str", choices=["immutable", "stable", "volatile"]),
        is_strict=dict(type="bool", default=False, aliases=["strict"]),
        is_security_definer=dict(
            type="bool", default=False, aliases=["security_definer"]
        ),
        cascade=dict(type="bool", default=False),
        session_role=dict(type="str"),
        trust_input=dict(type="bool", default=True),
    )
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
    )

    func = module.params["function"]
    state = module.params["state"]
    owner = module.params["owner"]
    newname = module.params["rename"]
    language = module.params["language"]
    return_type = module.params["return_type"]
    arguments = module.params["arguments"]
    source = module.params["source"]
    volatile = module.params["volatile"]
    strict = module.params["strict"]
    security_definer = module.params["security_definer"]
    cascade = module.params["cascade"]
    session_role = module.params["session_role"]
    trust_input = module.params["trust_input"]

    if not trust_input:
        # Check input for potentially dangerous elements
        check_input(
            module,
            func,
            owner,
            newname,
            language,
            return_type,
            arguments,
            source,
            volatile,
            session_role,
        )

    if state == "present" and cascade:
        module.warn("cascade=true is ignored when state=present")

    # Check mutual exclusions
    if state == "absent" and (
        newname
        or language
        or return_type
        or source
        or volatile
        or strict
        or security_definer
        or owner
    ):
        module.fail_json(
            msg="%s: state=absent is mutually exclusive with: "
            "language, return_type, source, volatile, "
            "strict, security_definer, owner" % func
        )

    if newname and (
        language
        or return_type
        or source
        or volatile
        or strict
        or security_definer
        or owner
    ):
        module.fail_json(
            msg="%s: rename is mutually exclusive with: "
            "language, return_type, source, volatile, "
            "strict, security_definer, owner" % func
        )

    # Ensure psycopg2 libraries are available before connecting to DB
    ensure_required_libs(module)
    conn_params = get_conn_params(module, module.params)
    db_connection, dummy = connect_to_db(module, conn_params, autocommit=False)
    cursor = db_connection.cursor(cursor_factory=DictCursor)

    # Convert arguments to conform with postgresql catalog returns
    if arguments:
        arguments = ", ".join(arguments)
    if volatile:
        volatile = volatile.upper()
        volatile = (
            "i" if volatile == "IMMUTABLE" else "s" if volatile == "STABLE" else "v"
        )
    else:
        volatile = "v"

    ##############
    # Do main job:

    func_obj = Function(func, module, cursor)

    # Set default return values
    changed = False
    kw = {}
    kw["function"] = func
    kw["state"] = ""
    if func_obj.exists:
        kw = dict(
            function=func,
            state="present",
            owner=func_obj.info["owner"],
            language=func_obj.info["language"],
            return_type=func_obj.info["return_type"],
            arguments=func_obj.info["arguments"],
            volatile=func_obj.info["volatile"],
            is_strict=func_obj.info["is_strict"],
            is_secdef=func_obj.info["is_secdef"],
            source=func_obj.info["source"],
        )

    if state == "absent":
        changed = func_obj.drop(cascade=cascade)

    elif newname:
        changed = func_obj.rename(newname)

    elif state == "present":
        changed = func_obj.create(
            language=language,
            return_type=return_type,
            source=source,
            arguments=arguments,
            volatile=volatile,
            is_strict=strict,
            is_secdef=security_definer,
        )

    if changed:
        if module.check_mode:
            db_connection.rollback()
        else:
            db_connection.commit()

        # Refresh function info for RETURN
        func_obj.get_info()
        db_connection.commit()
        if func_obj.exists:
            kw = dict(
                function=func,
                state="present",
                owner=func_obj.info["owner"],
                language=func_obj.info["language"],
                return_type=func_obj.info["return_type"],
                arguments=func_obj.info["arguments"],
                volatile=func_obj.info["volatile"],
                is_strict=func_obj.info["is_strict"],
                is_secdef=func_obj.info["is_secdef"],
                source=func_obj.info["source"],
            )
        else:
            # We just change the function state here
            # to keep other information about the dropped function
            kw["state"] = "absent"

    kw["queries"] = func_obj.executed_queries
    kw["changed"] = changed
    db_connection.close()
    module.exit_json(**kw)


if __name__ == "__main__":
    main()
