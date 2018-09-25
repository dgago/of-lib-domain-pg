import * as of from "of-lib-domain";
import { PoolClient } from "pg";

export abstract class PgDbStore<T extends of.Entity<K>, K>
  implements of.IStore<T, K> {
  private operands = [
    { $eq: "=" },
    { $gt: ">" },
    { $gte: ">=" },
    { $in: "IN" },
    { $lt: "<" },
    { $lte: "<=" },
    { $ne: "!=" },
    { $nin: "NOT IN" }
  ];

  constructor(
    protected client: PoolClient,
    protected tableName: string,
    protected fields: string[],
    protected pkName: string
  ) {}

  /**
   * Obtiene un item.
   */
  findOne(id: K): Promise<T> {
    const query = {
      text: `SELECT * FROM ${this.tableName} WHERE ${this.pkName} = $1`,
      values: [id]
    };

    return this.client.query(query).then((res) => {
      if (res.rowCount == 0) {
        return undefined;
      }

      return res.rows[0];
    });
  }

  /**
   * Obtiene todos los items que responden a la consulta.
   */
  findAll(q: of.IQuery): Promise<of.IResults<T>> {
    const query = this.getQueryConfig(q);

    return this.client.query(query).then((res) => {
      return {
        count: res.rowCount,
        items: res.rows,
        pageIndex: 1,
        pageSize: res.rowCount
      };
    });
  }

  /**
   * Obtiene una página de los items que responden a la consulta
   */
  find(
    q: of.IQuery,
    pageIndex: number = 0,
    pageSize: number = 0,
    sort: of.IQuery = {}
  ): Promise<of.IResults<T>> {
    const countQuery = this.getCountQueryConfig(q);
    const query = this.getQueryConfig(q, pageIndex, pageSize, sort);

    return this.client.query(countQuery).then((count) => {
      return this.client.query(query).then((res) => {
        return {
          count: parseInt(count.rows[0].count),
          items: res.rows,
          pageIndex: 1,
          pageSize: res.rowCount
        };
      });
    });
  }

  /**
   * Verifica la existencia de un item.
   */
  exists(q: of.IQuery): Promise<boolean> {
    const query = this.getQueryConfig(q);

    return this.client.query(query).then((res) => {
      return res.rowCount > 0;
    });
  }

  /**
   * Crea un item.
   */
  create(item: T): Promise<K> {
    const query = {
      text: `INSERT INTO ${
        this.tableName
      }(${this.getFieldNames()}) VALUES(${this.getOrderedList()}) RETURNING *`,
      values: this.getValueList(item, true)
    };

    return this.client.query(query).then((res) => {
      if (res.rowCount == 0) {
        return undefined;
      }

      return res.rows[0].id;
    });
  }

  replace(id: K, item: T): Promise<boolean> {
    const query = {
      text: `UPDATE ${this.tableName} SET ${this.getOrderedFieldValueList(
        false
      )} WHERE ${this.pkName} = '${id}'`,
      values: this.getValueList(item, false)
    };

    return this.client.query(query).then((res) => {
      if (res.rowCount == 0) {
        return false;
      }

      return true;
    });
  }

  delete(id: K): Promise<boolean> {
    const query = {
      text: `DELETE FROM ${this.tableName} WHERE ${this.pkName} = $1`,
      values: [id]
    };

    return this.client.query(query).then((res) => {
      return res.rowCount > 0;
    });
  }

  /**
   *
   * Private methods.
   *
   */

  private getFieldNames(): string {
    const res = this.fields.join(", ");
    return res;
  }

  private getOrderedList(): string {
    const array = Array.from(Array(this.fields.length).keys());

    let ret: string = "";
    for (const item of array) {
      ret += `, $${item + 1}`;
    }

    return ret.substr(1).trim();
  }

  private getValueList(item: T, includePk: boolean): Array<any> {
    const flds = includePk
      ? this.fields.slice()
      : this.fields.filter((x) => x !== this.pkName);

    const array = flds.map((value: string, idx: number, array: string[]) => {
      return item[value];
    });
    return array;
  }

  private getOrderedFieldValueList(includePk: boolean): string {
    const flds = includePk
      ? this.fields.slice()
      : this.fields.filter((x) => x !== this.pkName);

    let res = "",
      i = 0;
    for (const fld of flds) {
      res += `, ${fld} = $${++i}`;
    }

    return res.substr(1).trim();
  }

  private getCountQueryConfig(q: of.IQuery) {
    const text = `SELECT COUNT(*) FROM ${this.tableName} ${this.getWhereClause(
      q
    )}`.trim();
    return {
      text: text,
      values: this.getWhereValues(q)
    };
  }

  private getQueryConfig(
    q: of.IQuery,
    pageIndex: number = 0,
    pageSize: number = 0,
    sort: of.IQuery = {}
  ) {
    const text = `SELECT * FROM ${this.tableName} ${this.getWhereClause(
      q
    )} ${this.getPaginationClause(pageIndex, pageSize)} ${this.getSortingClause(
      sort
    )}`.trim();
    return {
      text: text,
      values: this.getWhereValues(q)
    };
  }

  getSortingClause(sort: of.IQuery): any {
    if (!sort) {
      return "";
    }

    const keys = Object.keys(sort);
    if (keys.length === 0) {
      return "";
    }

    let res: string = "";
    for (const key of keys) {
      const sortType = sort[key] > 0 ? "ASC" : "DESC";
      res += `, ${key} ${sortType}`;
    }

    return `ORDER BY ${res.substr(1).trim()}`;
  }

  getPaginationClause(pageIndex: number, pageSize: number): any {
    if (pageSize > 0 && pageIndex > 0) {
      return `LIMIT ${pageSize} OFFSET ${(pageIndex - 1) * pageSize}`;
    }

    if (pageSize > 0) {
      return `LIMIT ${pageSize}`;
    }

    return "";
  }

  private getWhereClause(q: of.IQuery): string {
    const keys = Object.keys(q);

    let res: string = "",
      idx = 0;
    for (const key of keys) {
      idx++;
      const op =
        typeof q[key] === "object"
          ? `${this.getOperand(q, key)} $${idx}`
          : `= $${idx}`;
      res += `${key} ${op} AND `;
    }

    if (res && res.length > 0) {
      return `WHERE ${res.substr(0, res.length - 4).trim()}`;
    }

    return res;
  }

  private getWhereValues(q: of.IQuery): any {
    const values = Object.values(q);

    const mapped = values.map((value: any, idx: number, array: string[]) => {
      return typeof value === "object" ? Object.values(value)[0] : value;
    });

    return mapped;
  }

  private getOperand(q: of.IQuery, cv: string) {
    const operandKey = Object.keys(q[cv])[0];
    return this.operands[operandKey];
  }

  /**
   *
   * Not implemented.
   *
   */
  findOneSync(id: K): T {
    throw new Error("Method not implemented.");
  }

  findAllSync(q: of.IQuery): of.IResults<T> {
    throw new Error("Method not implemented.");
  }

  findSync(
    q: of.IQuery,
    pageIndex: number,
    pageSize: number,
    sort: of.IQuery
  ): of.IResults<T> {
    throw new Error("Method not implemented.");
  }

  existsSync(q: of.IQuery): boolean {
    throw new Error("Method not implemented.");
  }

  createSync(item: T): K {
    throw new Error("Method not implemented.");
  }

  replaceSync(id: K, item: T): void {
    throw new Error("Method not implemented.");
  }

  deleteSync(id: K): void {
    throw new Error("Method not implemented.");
  }
}
