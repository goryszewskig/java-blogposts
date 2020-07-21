package de.martin.tafDemo;

import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.OracleConnection;

/**
 * Demonstrate the need for the OCI driver when trying to use Transparent
 * Application Failover
 *
 */
public class App {
	private final String thinURL = "jdbc:oracle:thin:/@taf_svc?TNS_ADMIN=/home/martin/tns";
	private final String ociURL = "jdbc:oracle:oci:/@taf_svc?TNS_ADMIN=/home/martin/tns";
	private final int iterations = 1000;

	private String driverType;

	public App(String driverType) {
		this.driverType = driverType;
	}

	/**
	 * This procedure is where all the work takes place. It connects to the database (either via oci or thin driver)
	 * and runs <em>iteration</em> iterations of a simple select to print connection details to the screen.
	 * 
	 * @throws SQLException
	 */
	public void doSomeWork() throws SQLException {
		OracleDataSource ods = new OracleDataSource();

		if (driverType.contentEquals("oci")) {
			ods.setURL(ociURL);
		} else {
			ods.setURL(thinURL);
		}

		OracleConnection connection = (OracleConnection) ods.getConnection();

		DatabaseMetaData dbmd = connection.getMetaData();
		System.out.println("Driver Name: " + dbmd.getDriverName());
		System.out.println("Driver Version: " + dbmd.getDriverVersion());
		System.out.println("Connection established as " + connection.getUserName());
		System.out.println("\n");

		// add instrumentation
		connection.setClientInfo("OCSID.MODULE", "TAF Demo");
		connection.setClientInfo("OCSID.ACTION", driverType + " driver");

		// prepare the main loop
		PreparedStatement stmt = connection
				.prepareStatement("select inst_id, sid, failover_type,failover_method, failed_over, module, action "
						+ " from gv$session where username = user and program not like 'oracle%'");
		ResultSet rs = null;

		for (int i = 0; i < iterations; i++) {

			try {
				rs = stmt.executeQuery();

				while (rs.next()) {

					System.out.printf(
							"inst_id: %d sid: %05d failover_type: %-10s failover_method: %-10s failed_over: %-5s module: %s action: %s",
							rs.getInt("inst_id"), rs.getInt("sid"), rs.getString("failover_type"),
							rs.getString("failover_method"), rs.getString("failed_over"), rs.getString("module"),
							rs.getString("action"));
					System.out.println();

				}

				rs.close();

			} catch (SQLException sqle) {
				System.out.println("SQLException while trying to get the session information: " + sqle);
			}

			// give me some time to do some damage
			try {
				Thread.sleep(2000);
			} catch (Exception e) { }
		}

		rs.close();
		stmt.close();
		connection.close();
		connection = null;
	}

	public static void main(String[] args) {

		System.out.println("About to start a demonstration using Transparent Application Failover");

		App theApp = null;

		if (args.length != 1) {
			throw new IllegalArgumentException(
					"Must call this programme with either 'oci' or 'thin' to indicate the driver");
		}

		// the action to be performed depends on the driver type passed on the command line
		if (args[0].equals("oci")) {
			theApp = new App("oci");
		} else if (args[0].equals("thin")) {
			theApp = new App("thin");
		} else {
			throw new IllegalArgumentException(
					"Must call this programme with either 'oci' or 'thin' to indicate the driver");
		}

		try {
			theApp.doSomeWork();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
