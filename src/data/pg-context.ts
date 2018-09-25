import { DbContext } from "of-lib-domain";
import { Pool, PoolClient } from "pg";

/**
 * Contexto de base de datos para PostgreSQL.
 */
export class PgDbContext extends DbContext {
  constructor(connectionString: string) {
    super(connectionString);
  }

  /**
   * Obtiene un cliente conectado a la base de datos.
   */
  public getClient(): Promise<any> {
    return this.getPool().connect();
  }

  /**
   * Obtiene el pool de conexiones a la base de datos.
   */
  private _pool: Pool;
  public getPool(): Pool {
    if (!this._pool) {
      this._pool = new Pool({
        connectionString: this.connectionString
      });
    }
    return this._pool;
  }

  /**
   * Obtiene una referencia a la base de datos por defecto.
   */
  public async getDb() {
    throw new Error("Method not implemented.");
  }

  /**
   * Libera una conexión a la base de datos.
   */
  public release(client: PoolClient) {
    return client.release();
  }

  /**
   * Cierra la conexión establecida por el cliente.
   */
  public async close(force?: boolean) {
    return this._pool.end();
  }
}
