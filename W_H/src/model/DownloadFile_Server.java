package phan1;

import java.sql.SQLException;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import connection.database.DBConfig;
import connection.database.DBLog;

public class DownloadFile_Server {
	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}
	//
	public void DownloadFie() throws ClassNotFoundException, SQLException {
		String[] connection = DBConfig.getConfigInformation();
		String id = connection[0];
		String fileName = connection[1];
		String hostName = connection[2];
		int port = Integer.parseInt(connection[3]);
		String userName = connection[4];
		String passWord = connection[5];
		String remotePath = connection[6];
		String localPath = connection[7];
		String status = connection[8];
		System.out.println(port);
		if(status.equals("no")) {
			CkSsh ssh = new CkSsh();
			CkGlobal ck = new CkGlobal();
			ck.UnlockBundle("Download");
			boolean success = ssh.Connect(hostName, port);
			if(success!= true) {
				System.out.println(ssh.lastErrorText());
				return;
			}
			//
			ssh.put_IdleTimeoutMs(5000);
			//
			success= ssh.AuthenticatePw(userName, passWord);
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				return;
			}
			//
			CkScp scp = new CkScp();
			success = scp.UseSsh(ssh);
			if (success != true) {
				System.out.println(scp.lastErrorText());
				return;
			}
			//
			String srcPath=hostName+":"+remotePath+"/"+fileName;
			System.out.println(srcPath);
			success = scp.DownloadFile(srcPath, localPath);
			if (success != true) {
				System.out.println(scp.lastErrorText());
				return;
			}

			System.out.println("Tải file thành công...");

			// Ngat ket noi
			ssh.Disconnect();
			DBLog.getLogInformation(id, fileName);
			
			System.out.println("Đã ghi vào table_log...");
		}
	}
	public static void main(String argv[]) throws ClassNotFoundException, SQLException {
		DownloadFile_Server load = new DownloadFile_Server();
		load.DownloadFie();
		
	}
}
