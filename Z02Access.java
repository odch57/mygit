package server;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Z02Access {

	private String kind, name, address, telephone, job, sql, clientId;
	private int age, priKey;
	
	private JSONObject jobj, jobj2;
	private JSONParser jpar;
	private JSONArray jary;
	
	private Z03DBconnPool pool = null;
	private Connection conn = null;
	private PreparedStatement psmt = null;
	private ResultSet rs = null;

	private static boolean quit = false;

	public Z02Access() {
		// Pooling DB 연결
		pool = Z03DBconnPool.getInstance(); 		//System.out.println("pool : " + pool);
	}
	
	public JSONObject receive(String inMsg) {

		try {

			conn = pool.getConnection();

			jpar = new JSONParser();
			jobj = (JSONObject)jpar.parse(inMsg);

			kind = (String)jobj.get("kKind");			//System.out.println("kind : " + kind);
			clientId = (String)jobj.get("kClientId");	//System.out.println("clientId : " + clientId);

			if(kind.equals("vIn")) {

				// (1) json getter
				name = (String)jobj.get("kName");
				address = (String)jobj.get("kAddress");
				telephone = (String)jobj.get("kTelephone");
				Number nAge = (Number)jobj.get("kAge");	age = nAge.intValue();

				// (2) SQL 문 실행 
				sql = "INSERT INTO juso VALUES(null, ?, ?, ?, ?)";
				psmt = conn.prepareStatement(sql);
				psmt.setString(1, name);
				psmt.setString(2, address);
				psmt.setString(3, telephone);
				psmt.setInt(4, age);
				psmt.executeUpdate();
				psmt.close();
				//conn.close();

				// (3) json putter
				jobj = new JSONObject();
				jobj.put("kKind", "vIn");
				jobj.put("kClientId", clientId);
				jobj.put("kJob", "done");
			} else if(kind.equals("vOut")) {

				// sql 실행 -> json putter  
				name = (String)jobj.get("kName"); 	//System.out.println("name : " + name);
				if(name==null)  name = "";
				jary = new JSONArray();

				if(name.equals("")) { // 전체 출력
					sql = "SELECT * FROM juso";
					psmt = conn.prepareStatement(sql);
					rs = psmt.executeQuery();

					while(rs.next()) {

						jobj2 = new JSONObject(); // jobj = recDto

						name = rs.getString(2);			jobj2.put("kName", name);				//System.out.println("name :"+name);
						address = rs.getString(3);		jobj2.put("kAddress", address);			//System.out.printf("%-25s", address);
						telephone = rs.getString(4);	jobj2.put("kTelephone", telephone);		//System.out.printf("%-20s", telephone);
						age = rs.getInt(5);				jobj2.put("kAge", age);					//System.out.printf("%-5d", age);

						jary.add(jobj2);	// jary = dataDto
					}
				} else { // 검색 출력

					sql = "SELECT * FROM juso WHERE name LIKE ?";
					psmt = conn.prepareStatement(sql, rs.TYPE_SCROLL_SENSITIVE, rs.CONCUR_UPDATABLE);
					psmt.setString(1, "%"+name+"%");
					rs = psmt.executeQuery();

					if(!rs.next()) { // 검색 레코드가 없을 때

						jobj2 = new JSONObject();

						jobj2.put("kKind", "vOut");
						jobj2.put("kJob", "none");
					} else {

						rs.beforeFirst();	// rs의 1번째 레코드에 위치	//if(!name.equals(""))	
						while(rs.next()) {

							jobj2 = new JSONObject(); // jobj = recDto

							name = rs.getString(2);			jobj2.put("kName", name);				//System.out.println("name :"+name);
							address = rs.getString(3);		jobj2.put("kAddress", address);			//System.out.printf("%-25s", address);
							telephone = rs.getString(4);	jobj2.put("kTelephone", telephone);		//System.out.printf("%-20s", telephone);
							age = rs.getInt(5);				jobj2.put("kAge", age);					//System.out.printf("%-5d", age);

							jary.add(jobj2);	// jary = dataDto
						}
					}
				}
				rs.close();
				psmt.close();
				
				jobj = new JSONObject();
				jobj.put("kArray", jary);
				jobj.put("kKind", "vOut");
				jobj.put("kClientId", clientId);
				jobj.put("kJob", "done");
			} if(kind.equals("vTotal")) {

				// (1) sql 실행
				String sql = "SELECT * FROM juso";
				psmt = conn.prepareStatement(sql);
				rs = psmt.executeQuery();
				int total = 0;
				while(rs.next()) {
					total = total + 1;
				}
				rs.close();

				// (2) json putter
				jobj = new JSONObject();
				jobj.put("kKind", "vTotal");
				jobj.put("kTotal", total);	//System.out.println("total : " + total);
				jobj.put("kClientId", clientId);
				jobj.put("kJob", "done");

			} if(kind.equals("vLoad")) {

				// (1) json getter
				Number nNum = (Number)jobj.get("kNum");		int num = nNum.intValue();

				// (2) SQL 문 실행 -> json putter
				sql = "SELECT * FROM juso";

				psmt = conn.prepareStatement(sql);
				rs = psmt.executeQuery();

				int recNum = 1; // 레코드 번호
				while(rs.next()) {
					if(recNum==num) {
						priKey = rs.getInt(1);
						break;
					}
					recNum++;
				}

				sql = "SELECT * FROM juso WHERE pk = ?";
				psmt = conn.prepareStatement(sql);
				psmt.setInt(1, priKey);
				rs = psmt.executeQuery();

				if(!rs.next()) {

					jobj = new JSONObject();
					jobj.put("kKind", "vLoad");
					jobj.put("kJob", "none");
					rs.close();
					psmt.close();
				}

				jobj = new JSONObject();

				name = rs.getString(2);			jobj.put("kName", name);				//System.out.println("name :"+name);
				address = rs.getString(3);		jobj.put("kAddress", address);			//System.out.printf("%-25s", address);
				telephone = rs.getString(4);	jobj.put("kTelephone", telephone);		//System.out.printf("%-20s", telephone);
				age = rs.getInt(5);				jobj.put("kAge", age);					//System.out.println("age :"+age);
												jobj.put("kPriKey", priKey);			//System.out.println("priKey :"+priKey);

				rs.close();
				psmt.close();

				jobj.put("kKind", "vLoad");
				jobj.put("kClientId", clientId);
				jobj.put("kJob", "done");
				
			} if(kind.equals("vUp")) {
				
				// (1) json getter
				name = (String)jobj.get("kName");												//System.out.println("name : " + name);
				address = (String)jobj.get("kAddress");
				telephone = (String)jobj.get("kTelephone");
				Number nAge = (Number)jobj.get("kAge");			age = nAge.intValue();
				Number nPriKey = (Number)jobj.get("kPriKey");	priKey = nPriKey.intValue();	//System.out.println("priKey : " + priKey);

				// (2) SQL 문 실행
				sql = "UPDATE juso SET name = ?, address = ?, telephone = ?, age =? WHERE pk = ?";
				psmt = conn.prepareStatement(sql);
				psmt.setString(1, name);
				psmt.setString(2, address);
				psmt.setString(3, telephone);
				psmt.setInt(4, age);
				psmt.setInt(5, priKey);
				psmt.executeUpdate();
				psmt.close();
				//conn.close();

				// (3) json putter
				jobj = new JSONObject();
				jobj.put("kKind", "vUp");
				jobj.put("kClientId", clientId);
				jobj.put("kJob", "done");

			} if(kind.equals("vDel")) {

				// (1) json getter
				Number nPriKey = (Number)jobj.get("kPriKey");	priKey = nPriKey.intValue();

				// (2) SQL 문 실행
				String sql = "DELETE FROM juso WHERE pk = ?";
				psmt = conn.prepareStatement(sql);
				psmt.setInt(1, priKey);
				psmt.executeUpdate();
				psmt.close();

				// (3) json putter
				jobj = new JSONObject();
				jobj.put("kKind", "vDel");
				jobj.put("kClientId", clientId);
				jobj.put("kJob", "done");

			} if(kind.equals("vQuit")) {
				quit = true;
			}
		} catch (Exception e) {
				e.printStackTrace();
		} finally {
			// conn 반납 : pstm과 rs 객체 닫기
			pool.freeConnection(conn, psmt, rs);
		}
		return jobj;
	}
	
	public String getClientId() {
		return clientId;
	}

	public boolean getQuit() {
		return quit;
	}
}
