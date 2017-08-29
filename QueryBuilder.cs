using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace DataAccessLayer
{
    enum QueryType
    {
        Select,
        Insert,
        Update,
        Delete
    }

    enum EvalOperator
    {
        Equals,
        Greater,
        GreaterOrEqual,
        Less,
        LessOrEqual,
        Different,
        Like,
        In,
        Between
    }

    enum AppendOperator
    {
        Comma,
        And,
        Or
    }

    enum OrderType
    {
        Unordered,
        Ascending,
        Descending
    }

    enum JoinType
    {
        InnerJoin,
        LeftJoin,
        RightJoin
    }

    public class QueryBuilder
    {
        class AliasableName
        {
            private string _Name;

            public string Name
            {
                get
                {
                    if (!this.Alias.Equals(""))
                        return this.Alias;

                    return this._Name;
                }
                set { this._Name = value; }
            }

            public string Alias { get; set; }

            public AliasableName(string name, string alias = "")
            {
                this.Name = name;
                this.Alias = alias;
            }

            public override string ToString()
            {
                return Name + (Alias.Equals("") ? " as " + Alias : "");
            }
        }

        class ColValuePair
        {
            public string Col { get; set; }
            public List<object> Values { get; set; }
            public EvalOperator Operator { get; set; }
            public AppendOperator Appender { get; set; }

            public ColValuePair(EvalOperator op, string column, List<object> value, AppendOperator appender)
            {
                this.Col = column;
                this.Values = value;
                this.Operator = op;
            }

            public ColValuePair(EvalOperator op, string column, object value, AppendOperator appender)
            {
                this.Col = column;
                this.Operator = op;
                this.Values = new List<object>();
                this.Values.Add(value);
            }

            public string Compile(ref SqlCommand cmd)
            {
                if (this.Values.Count == 0)
                    return "";

                StringBuilder str = new StringBuilder();
                str.Append(Col);

                for (var i = 0; i < this.Values.Count; i++)
                    this.Values[i] = QueryBuilder.Escape(this.Values[i]);

                switch (this.Operator)
                {
                    case EvalOperator.Equals:
                        str.AppendFormat(" = {0}",
                                QueryBuilder.AddParameter(ref cmd, this.Col, this.Values[0])
                            );
                        break;
                    case EvalOperator.Greater:
                        str.AppendFormat(" > {0}",
                                QueryBuilder.AddParameter(ref cmd, this.Col, this.Values[0])
                            );
                        break;
                    case EvalOperator.GreaterOrEqual:
                        str.AppendFormat(" >= {0}",
                                QueryBuilder.AddParameter(ref cmd, this.Col, this.Values[0])
                            );
                        break;
                    case EvalOperator.Less:
                        str.AppendFormat(" < {0}",
                                QueryBuilder.AddParameter(ref cmd, this.Col, this.Values[0])
                            );
                        break;
                    case EvalOperator.LessOrEqual:
                        str.AppendFormat(" <= {0}",
                                QueryBuilder.AddParameter(ref cmd, this.Col, this.Values[0])
                            );
                        break;
                    case EvalOperator.Different:
                        str.AppendFormat(" <> {0}",
                                QueryBuilder.AddParameter(ref cmd, this.Col, this.Values[0])
                            );
                        break;
                    case EvalOperator.Like:
                        str.AppendFormat(" LIKE {0}",
                                QueryBuilder.AddParameter(ref cmd, this.Col, this.Values[0])
                            );
                        break;
                    case EvalOperator.Between:
                        str.AppendFormat(" BETWEEN {0} AND {1}",
                                this.Values[0],
                                this.Values.Count > 1 ? this.Values[1] : this.Values[0]
                            );
                        break;
                    case EvalOperator.In:
                        str.AppendFormat(" IN ({0})",
                                string.Join(",", this.Values.ToArray())
                            );
                        break;
                }

                return str.ToString();
            }

            public string CompileWithAppender(ref SqlCommand cmd)
            {
                StringBuilder str = new StringBuilder();

                if (this.Appender == AppendOperator.Comma)
                    str.Append(", ");
                else
                    str.AppendFormat("{0} ", this.Appender.ToString().ToUpper());

                str.Append(Compile(ref cmd));

                return str.ToString();
            }
        }

        struct JoinClause
        {
            public AliasableName JoiningTable;
            public string SourceColumn;
            public string JoiningColumn;
            public JoinType Type;

            public JoinClause(JoinType type, string table, string sourceCol, string joinCol, string tableAlias = "")
            {
                this.Type = type;
                this.JoiningTable = new AliasableName(table, tableAlias);
                this.SourceColumn = sourceCol;
                this.JoiningColumn = joinCol;
            }
        }

        private QueryType QType;
        private AliasableName Table;
        private List<AliasableName> Columns = new List<AliasableName>();
        private List<ColValuePair> Conditions = new List<ColValuePair>();
        private List<ColValuePair> Values = new List<ColValuePair>();
        private List<JoinClause> Joins = new List<JoinClause>();
        private List<KeyValuePair<string, OrderType>> OrderByColumns = new List<KeyValuePair<string, OrderType>>();
        private List<string> GroupByColumns = new List<string>();

        public QueryBuilder(string table, string tableAlias = "")
        {
            this.Table = new AliasableName(table, tableAlias);
        }

        private void ProcessConditions(ref StringBuilder cmdText, ref SqlCommand cmd)
        {
            if (this.Conditions.Count == 0)
                return;

            cmdText.Append(" WHERE ");
            cmdText.Append(this.Conditions[0].Compile(ref cmd));

            if (this.Conditions.Count > 1)
            {
                for (var i = 1; i < this.Conditions.Count; i++)
                {
                    ColValuePair cond = this.Conditions[i];
                    cmdText.Append(cond.CompileWithAppender(ref cmd));
                }
            }
        }

        private void ProcessJoins(ref StringBuilder cmdText)
        {
            if (this.Joins.Count == 0)
                return;

            foreach (JoinClause join in this.Joins)
                cmdText.AppendFormat(" JOIN {0} ON {1}.{2} = {0}.{3}", join.JoiningTable, this.Table.Name, join.SourceColumn, join.JoiningColumn);
        }

        private void ProcessOrders(ref StringBuilder cmdText)
        {
            if (this.OrderByColumns.Count == 0)
                return;

            cmdText.AppendFormat(" ORDER BY {0} {1}", this.OrderByColumns[0].Key, this.OrderByColumns[0].Value.ToString().ToUpper());

            if (this.OrderByColumns.Count > 1)
            {
                for (var i = 1; i < this.OrderByColumns.Count; i++)
                    cmdText.AppendFormat(", {0} {1}", this.OrderByColumns[i].Key, this.OrderByColumns[i].Value.ToString().ToUpper());
            }
        }

        private void ProcessGroups(ref StringBuilder cmdText)
        {
            if (this.GroupByColumns.Count == 0)
                return;

            cmdText.AppendFormat(" GROUP BY {0}", this.GroupByColumns[0]);

            if (this.GroupByColumns.Count > 1)
            {
                for (var i = 1; i < this.GroupByColumns.Count; i++)
                    cmdText.AppendFormat(", {0}", this.GroupByColumns[i]);
            }
        }

        protected static string AddParameter(ref SqlCommand cmd, string col, object val)
        {
            col = "@" + col.Replace(".", "").Replace("_", "") + "_col";
            cmd.Parameters.AddWithValue(col, val);

            return col;
        }

        protected static string AddParameter(ref SqlCommand cmd, string col, object val, SqlDbType type)
        {
            col = "@" + col.Replace(".", "").Replace("_", "")  + "_col";
            cmd.Parameters.Add(col, type).Value = val;

            return col;
        }

        protected static string Escape(object val)
        {
            return (val is string ? "'" + val + "'" : val.ToString());
        }

        public SqlCommand CompileCommand()
        {
            SqlCommand cmd = new SqlCommand();
            StringBuilder str = new StringBuilder();

            switch (QType)
            {
                case QueryType.Select:
                    str.AppendFormat("SELECT {0} FROM {1}",
                            string.Join(",", this.Columns.Select(c => c.ToString()).ToArray()),
                            this.Table.ToString()
                        );
                    ProcessJoins(ref str);
                    ProcessConditions(ref str, ref cmd);
                    ProcessGroups(ref str);
                    ProcessOrders(ref str);
                    break;
                case QueryType.Insert:
                    str.AppendFormat("INSERT INTO {0} ({1}) VALUES ({2})",
                            this.Table.Name,
                            string.Join(",", this.Values.Select(p => p.Col).ToArray()),
                            string.Join(",", this.Values.Select(p => Escape(p.Values[0])).ToArray())
                        );
                    break;
                case QueryType.Update:
                    if (this.Values.Count == 0)
                        throw new ArgumentException("No values specified for SQL Update");

                    str.AppendFormat("UPDATE {0} SET", this.Table.Name);
                    str.Append(this.Values[0].Compile(ref cmd));

                    if (this.Values.Count > 1)
                        for (var i = 1; i < this.Conditions.Count; i++)
                        {
                            ColValuePair cond = this.Conditions[i];
                            str.Append(cond.CompileWithAppender(ref cmd));
                        }

                    ProcessConditions(ref str, ref cmd);
                    break;
                case QueryType.Delete:
                    // (song) No te olvides de poner el where en el delete from ... (song)
                    if (this.Conditions.Count == 0)
                        throw new ArgumentException("No se puede ejecutar una consulta SQL DELETE sin condiciones en el segmento WHERE");

                    str.AppendFormat("DELETE FROM {0}");
                    ProcessConditions(ref str, ref cmd);
                    break;
            }

            // Fin de consulta
            str.Append(";");
            cmd.CommandText = str.ToString();

            return cmd;
        }

        public QueryBuilder Select(string columnOrList)
        {
            this.QType = QueryType.Select;
            this.Columns.Add(new AliasableName(columnOrList));

            return this;
        }

        public QueryBuilder Select(string column, string alias)
        {
            this.QType = QueryType.Select;
            this.Columns.Add(new AliasableName(column, alias));

            return this;
        }

        public QueryBuilder Select(string[] columns)
        {
            this.QType = QueryType.Select;

            foreach (string col in columns)
                this.Columns.Add(new AliasableName(col));

            return this;
        }

        public QueryBuilder Insert(string[] columns, object[] values)
        {
            if (columns.Length != values.Length)
                throw new ArgumentException("El número de columnas y el número de valores no coinciden para SQL Insert");

            this.QType = QueryType.Insert;

            for (var i = 0; i < columns.Length; i++)
                this.Values.Add(new ColValuePair(EvalOperator.Equals, columns[i], values[i], AppendOperator.Comma));

            return this;
        }

        public QueryBuilder Insert(Dictionary<string, object> values)
        {
            this.QType = QueryType.Insert;

            foreach (KeyValuePair<string, object> val in values)
                this.Values.Add(new ColValuePair(EvalOperator.Equals, val.Key, val.Value, AppendOperator.Comma));

            return this;
        }

        public QueryBuilder Update(string column, object value)
        {
            this.QType = QueryType.Update;

            this.Values.Add(new ColValuePair(EvalOperator.Equals, column, value, AppendOperator.Comma));

            return this;
        }

        public QueryBuilder Update(Dictionary<string, object> values)
        {
            this.QType = QueryType.Update;

            foreach (KeyValuePair<string, object> val in values)
                this.Values.Add(new ColValuePair(EvalOperator.Equals, val.Key, val.Value, AppendOperator.Comma));

            return this;
        }

        public QueryBuilder Delete()
        {
            this.QType = QueryType.Delete;

            return this;
        }

        public QueryBuilder Delete(string whereColumn, object value)
        {
            this.QType = QueryType.Delete;
            this.Conditions.Add(new ColValuePair(EvalOperator.Equals, whereColumn, value, AppendOperator.And));

            return this;
        }

        public QueryBuilder From(string table, string alias = "")
        {
            this.Table = new AliasableName(table, alias);

            return this;
        }

        public QueryBuilder LeftJoin(string joinTable, string sourceCol, string joinCol, string tableAlias = "")
        {
            this.Joins.Add(new JoinClause(JoinType.LeftJoin, joinTable, sourceCol, joinCol, tableAlias));

            return this;
        }

        public QueryBuilder RightJoin(string joinTable, string sourceCol, string joinCol, string tableAlias = "")
        {
            this.Joins.Add(new JoinClause(JoinType.RightJoin, joinTable, sourceCol, joinCol, tableAlias));

            return this;
        }

        public QueryBuilder InnerJoin(string joinTable, string sourceCol, string joinCol, string tableAlias = "")
        {
            this.Joins.Add(new JoinClause(JoinType.InnerJoin, joinTable, sourceCol, joinCol, tableAlias));

            return this;
        }

        public QueryBuilder Where(string column, object value, AppendOperator andOr = AppendOperator.And)
        {
            this.Conditions.Add(new ColValuePair(EvalOperator.Equals, column, value, andOr));

            return this;
        }

        public QueryBuilder WhereIn(string column, List<object> values, AppendOperator andOr = AppendOperator.And)
        {
            this.Conditions.Add(new ColValuePair(EvalOperator.In, column, values, andOr));

            return this;
        }

        public QueryBuilder WhereBetween(string column, object start, object end, AppendOperator andOr = AppendOperator.And)
        {
            List<object> limits = new List<object>();
            limits.Add(start);
            limits.Add(end);
            this.Conditions.Add(new ColValuePair(EvalOperator.Between, column, limits, andOr));

            return this;
        }

        public QueryBuilder OrderBy(string column, OrderType order = OrderType.Ascending)
        {
            KeyValuePair<string, OrderType> ord = new KeyValuePair<string, OrderType>(column, order);
            this.OrderByColumns.Add(ord);

            return this;
        }

        public QueryBuilder GroupBy(string column)
        {
            this.GroupByColumns.Add(column);

            return this;
        }

        public QueryBuilder GroupBy(string[] columns)
        {
            this.GroupByColumns.AddRange(columns);

            return this;
        }
    }
}
