#include <iostream>
#include <cstdio>
#include <sstream>
#include <string.h>
#include <stdio.h>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>

class dataBase
{
private:
	std::string path = "/home/kali/DataBase/DBfiles/scheme1/";
	std::string path2 = "/home/kali/DataBase/DBfiles/";
	char deth0 = '0';
	std::string Tables;

	//----------------------------------------------Работа с таблицами------------------------------------------------------------
	std::string lock_get(std::string table_lock){
		std::ifstream file("/home/kali/DataBase/schema.json", std::ifstream::binary);
		nlohmann::json Json;
		file >> Json;
		file.close();
		std::string abvg = Json["name"];
		std::string NewPath = "/home/kali/DataBase/DBfiles/" + abvg + "/" + table_lock + "/" + table_lock + "_lock.txt";
		std::string getlock;
		std::ifstream lock(NewPath);
		lock >> getlock;
		lock.close();
		return getlock;
	}

	void lock(std::string table_lock){
		std::ifstream file("/home/kali/DataBase/schema.json", std::ifstream::binary);
		nlohmann::json Json;
		file >> Json;
		file.close();
		std::string abvg = Json["name"];
		std::string NewPath = "/home/kali/DataBase/DBfiles/" + abvg + "/" + table_lock + "/" + table_lock + "_lock.txt";

		std::ofstream lock(NewPath, std::ios::out);
		lock << 1;
		lock.close();
	}

	void unlock(std::string table_lock){
		std::ifstream file("/home/kali/DataBase/schema.json", std::ifstream::binary);
		nlohmann::json Json;
		file >> Json;
		file.close();
		std::string abvg = Json["name"];
		std::string NewPath = "/home/kali/DataBase/DBfiles/" + abvg + "/" + table_lock + "/" + table_lock + "_lock.txt";

		std::ofstream lock(NewPath, std::ios::out);
		lock << 1;
		lock.close();
	}

	void CreatTables(std::string structur)
	{

		std::cout << structur;

		std::string Line;
		for (int i = 0; i < structur.size(); i++)
		{
			if (structur[i] >= 'a' && structur[i] <= 'z' || structur[i] >= '0' && structur[i] <= '9')
			{
				Line += structur[i];
			}
			else
			{
				for (const auto& entry : std::filesystem::recursive_directory_iterator(path))
				{
					if (entry.path().stem() == Line)
					{

					}
				}
			}
		}


		if (!std::filesystem::exists(path)) {
			std::cerr << "Директория не существует." << std::endl;
			return;
		}

		// Обход директорий
		std::cout << "\n\nScheme DataBase\n\n";
		for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) { //вывод всей структуры
			if (entry.path().extension() == ".csv")
			{

				std::fstream f(entry.path(), std::ios_base::app);

				f << "1;2\n3";

				f.close();
			}
		}
	}
	//----------------------------------------------Создание директорий под хранение БД--------------------------------------------
	void CreatDataBase()
	{
		std::ifstream file("/home/kali/DataBase/schema.json", std::ifstream::binary);
		nlohmann::json Json;
		file >> Json;
		file.close();
		std::string abvg = Json["name"];
		std::string NewPath = "/home/kali/DataBase/DBfiles/" + abvg;
		
		std::filesystem::create_directories(NewPath);

		for (const auto& element : Json["structure"].items()) {

			
			auto b = element.key();

			if (std::filesystem::exists(NewPath + "/" + b) && std::filesystem::is_directory(NewPath + "/" + b))
				continue;
			

			std::filesystem::create_directories(NewPath + "/" + b);
			std::ofstream op(NewPath + "/" + b + "/1.csv");
			std::string a = NewPath + "/" + b + "/1.csv";
			op << b + "_pk;";

			for (auto element2 : element.value())
			{
				std::string newcol = "";

				for (auto c : element2)
				{
					if (c != '\"')
						newcol += c;
				}
				op << newcol << ";";
			}
			op << "\n";
			op.close();

			std::ofstream od(NewPath + "/" + b + "/" + b + "_pk_sequence.txt");
			od<< 0;
			od.close();

			std::ofstream oe(NewPath + "/" + b + "/" + b + "_lock.txt");
			oe << 0;
			oe.close();

		}
	}
	int CountPillars(std::string line)
	{
		int count = 0;
		std::stringstream ss(line);
		std::string value;
		int currentColumn = 0;
		while (std::getline(ss, value, ';'))
		{
			count++;
		}
		return count;
	}

	void ClearCash()
	{
		std::string g0 = "/home/kali/DataBase/DBfiles/temp0.csv";
		std::string path = "/home/kali/DataBase/DBfiles/temp";
		std::string g = ".csv";
		char deth = '1';
		remove(g0.c_str());
		while (std::filesystem::exists(path + deth + g))
		{
		std::ofstream t(path + deth + g, std::ios::trunc);
			std::string gg = path + deth + g;
			t.close();
			remove(gg.c_str());
			deth++;
		}
	}
	//----------------------------------------------Вывод точена Select---------------------------------------------
	void OutputSelect(char deth, std::string privcol)
	{
		std::string privprivcol;
		int maxcol1 = 0, maxcol2 = 0;
		int temp = 1;
		int targetColumn = 0;
		int col = 0;
		std::string g = ".csv";
		std::string path = "/home/kali/DataBase/DBfiles/temp";
		do
		{
			temp = 0;
			char next = deth + 1;
			std::string line2;
			std::fstream t(path + deth + g);
			if (std::filesystem::exists(path + next + g))
			{
				while (std::getline(t, line2))
				{
					std::stringstream ss(line2);
					if (col < CountPillars(line2))
						col = CountPillars(line2);
					std::string value;
					int currentColumn = 0;
					while (std::getline(ss, value, ';'))
					{

						if (currentColumn == targetColumn)
						{
							temp = 1;
							privprivcol += value + "\t";
							break;
						}
						currentColumn++;
					}
					maxcol1 = currentColumn;

				}
				if (temp != 0)
					OutputSelect(next, privcol + privprivcol);
				privprivcol = "";

			}
			else
			{
				std::cout << privcol << "\t";
				while (std::getline(t, line2))
				{
					if(col < CountPillars(line2))
						col = CountPillars(line2);
					std::stringstream ss(line2);
					std::string value;
					int currentColumn = 0;
					while (std::getline(ss, value, ';'))
					{

						if (currentColumn == targetColumn)
						{

							temp = 1;
							std::cout << value << "\t";
							break;
						}
						currentColumn++;

					}
					maxcol2 = currentColumn;
				}
			}

			t.close();

			targetColumn++;
			std::cout << "\n";
		} while (col > targetColumn);
	}
	std::string findValue(int deep, std::string value)
	{
		std::ifstream file("/home/kali/DataBase/schema.json", std::ifstream::binary);
		nlohmann::json Json;
		file >> Json;
		file.close();
		std::string abvg = Json["name"];

		std::string table;
		std::string sf;
		std::string column;
		int temp = 0;
		int temp2 = 0;
		int temp3 = 0;

		value += '!';
		for (auto temp : value)
		{
			if (temp == '.')
			{
				table = sf;
				sf = "";
				continue;
			}
			if (temp == '!')
			{

				column = sf;
				sf = "";
				break;
			}
			sf += temp;
		}
		std::string NewPath = "/home/kali/DataBase/DBfiles/" + abvg +"/" + table + "/" + "1.csv";
		std::filesystem::path temp_path;
		if (std::filesystem::exists(NewPath) && std::filesystem::is_regular_file(NewPath)) {
			temp_path = NewPath;
		}
		else {
			std::cerr << "Dir not found" << std::endl;
		}

	
		std::ifstream inFile(temp_path);
		std::string line;
		int a = 0;
		if (column != "")
		{
			while (std::getline(inFile, line))
			{
				std::stringstream ss(line);
				std::string value;
				int currentColumn = 0;
				while (std::getline(ss, value, ';')) {


					if (value == column)
					{
						a = 1;
						break;
					}

					if (currentColumn == temp2 && a == 1 && temp3 == deep) {

						return value;
					}
					if (a == 0)
						temp2++;

					currentColumn++;

				}
				temp3++;
			}
		}
		return "";
	}
		
	int Where3rd(int deep, std::string Line)
	{
		std::stringstream ss(Line);
		std::string firstColumn;
		std::string secondColumn;
		std::string op;
		ss >> firstColumn >> op >> secondColumn;

		std::string firstValue = findValue(deep, firstColumn);
		std::string secondValue = findValue(deep, secondColumn);

		if (firstValue != secondValue)
			return 0;
		else
			return 1;
	}
	int Where2nd(int deep, std::string Line)
	{
		std::stringstream ss(Line);
		std::string value;
		std::string tempLine;
		int checked = 3;

		while (std::getline(ss, value, ' '))
		{
			if (value == "and")
			{
				checked = Where3rd(deep, tempLine);
				tempLine = "";
				
			}
			tempLine += value + " ";
			if (checked == 0)
				return 0;
		}
		checked = Where3rd(deep, tempLine);
		if (checked == 0)
			return 0;

		return 1;
	}
	int Where1st(int deep, std::string line)
	{
		std::stringstream ss(line);
		std::string value;
		std::string tempLine;
		std::string secondLine;
		int checked;

		while (std::getline(ss, value, ' '))
		{
			if (value == "or")
			{
				checked = Where2nd(deep, tempLine);
				if (checked == 1)
					return 1;
				tempLine = "";
			}
			tempLine += value + " ";
		}

		checked = Where2nd(deep, tempLine);

		if (checked == 1)
			return 1;

		return 0;
	}
	void Table(std::string Table, std::string Column, std::string whereLine) 
	{
		std::ifstream file("/home/kali/DataBase/schema.json", std::ifstream::binary);
		nlohmann::json Json;
		file >> Json;
		file.close();
		std::string abvg = Json["name"];
		std::string NewPath = "/home/kali/DataBase/DBfiles/" + abvg +"/" + Table + "/" + "1.csv";
		int temp = 0;
		int temp2 = 0;
		if (Tables != Table) { deth0 += 1; Tables = Table; }
		std::filesystem::path temp_path;

		std::string TempFile = ".csv";
		std::string Path10 = "/home/kali/DataBase/DBfiles/temp";

		if (std::filesystem::exists(NewPath) && std::filesystem::is_regular_file(NewPath)) {
			temp_path = NewPath;
		}
		else {
			std::cerr << "Dir not found" << std::endl;
			return;
		}
				
		std::ifstream inFile(temp_path);
		
		std::fstream tempFile(Path10 + deth0 + TempFile, std::ios::app);
		std::string line;
		int a = 0;
		int currentRow = 0;
		if (Column != "")
		{
			while (std::getline(inFile, line))
			{
				std::stringstream ss(line);
				std::string value;
				int currentColumn = 0;
				while (std::getline(ss, value, ';')) {


					if (value == Column)
					{
						a = 1;
						break;
					}

					if (currentColumn == temp2 && a == 1) {
						if (whereLine != "") { if (Where1st(currentRow, whereLine) == 1) { tempFile << value << ";"; } }
						else
							tempFile << value << ";";
						break;
					}
					if (a == 0)
						temp2++;

					currentColumn++;

				}
				currentRow++;
			}
		}
		else
		{
			bool check = 0;
			temp2 = 0;
			do
			{


				int currentRow = 0;
				int currentColumn = 0;
				while (std::getline(inFile, line))
				{
					std::stringstream ss(line);
					std::string value;
					
					while (std::getline(ss, value, ';')) {


						if (currentColumn == temp2) {

							tempFile << value << ";";
							break;
						}
						temp2++;

						

					}
				}
				currentColumn++;
			} while (check == 0);
		}
		//tempFile << ";";
		inFile.close();
		tempFile << "\n";
		temp = 0;

	}
	//----------------------------------------------Токен Select----------------------------------------------------------------
	void Select(std::string Line/*, std::filesystem::path a, char deth*/)
	{

		std::stringstream ss(Line);
		std::stringstream ss2(Line);
		std::string value;
		std::string temp;
		std::string sf = "";
		std::string table;
		std::string column;
		std::string whereLine = "";
		bool columnCheck = 0;
		int a = 0;

		while (std::getline(ss2, value, ' '))
		{
			if (value == "where") {
				a = 1;
				continue;
			}
			if (a == 1)
				whereLine += value + " ";
		}


		while (std::getline(ss, value, ' '))
		{
			
			if (value == "from")
				break;
			if (value == "where")
				break;
			value += '!';
			for (auto temp : value)
			{
				if (temp == '.')
				{
					columnCheck = 1;
					table = sf;
					sf = "";
					continue;
				}
				if (temp == '!' || temp == ',')
				{
					if(lock_get(table) == "1")
						while(lock_get() == "1"){}
					else
						lock(table);
					Table(table, sf, whereLine);
					sf = "";
					break;
				}
				sf += temp;
			}
		}

		char deth;
		std::string pr;
		OutputSelect(deth = '1', pr = "");

		unlock(table);

	}
	
	void InsertInto(std::stringstream& Line1)
	{

		

		std::string part;
		Line1 >> part;

		std::ifstream file("/home/kali/DataBase/schema.json", std::ifstream::binary);
		nlohmann::json Json;
		file >> Json;
		file.close();
		std::string abvg = Json["name"];
		std::string NewPath = "/home/kali/DataBase/DBfiles/" + abvg +"/" + part + "/" + "1.csv";
		std::string NewDir = "/home/kali/DataBase/DBfiles/" + abvg +"/" + part + "/" + part + "_pk_sequence.txt";
		if(lock_get(table) == "1")
			while(lock_get() == "1"){}
		else
			lock(table);
		int temp = 0;
		std::filesystem::path a;

		if (std::filesystem::exists(NewPath) && std::filesystem::is_regular_file(NewPath)) {
			a = NewPath;
		}
		else {
			std::cerr << "Dir not found" << std::endl;
			return;
		}

		std::string Line;
		std::string value;
		Line1 >> Line;

		if (Line == "values")
		{
			
			do
			{
				Line1 >> Line;
				value += Line;
			}while (!Line1.eof());
		}
		else
		{
			std::cerr << "value command not found" << std::endl;
			return;
		}
		
		std::ifstream f_numb(NewDir);
		int numb;
		f_numb >> numb;
		f_numb.close();

		std::fstream f_insert(NewDir, std::ios::out);
		f_insert << ++numb;
		f_insert.close();

		std::fstream ff_insert(a, std::ios_base::app);
		ff_insert << numb << ";";
		ff_insert.close();

		std::string part_temp = "";
		for (auto iterator = 0; iterator != value.size(); iterator++)
		{

			if (value[iterator] == '\"' && temp == 0)
			{
				temp = 1;
			}
			else
				if (value[iterator] == '\"' && temp == 1)
				{
					temp = 0;
					std::fstream f(a, std::ios_base::app);

					f << part_temp << ';';

					part_temp = "";

					f.close();

				}

			if (temp == 1 && value[iterator] != '\"')
			{
				part_temp += value[iterator];
			}
		}

		std::fstream f(a, std::ios_base::app);


		f << "\n";

		f.close();

		unlock(part);

	}
	void deleteFrom(std::string Line)
	{
		std::ifstream file("/home/kali/DataBase/schema.json", std::ifstream::binary);
		nlohmann::json Json;
		file >> Json;
		file.close();
		std::string abvg = Json["name"];

		std::string tabcol;
		std::string table;
		std::string sf;
		std::string column;
		int temp = 0;
		int temp2 = 0;
		int temp3 = 0;

		std::string filePath;
		std::string s;
		std::stringstream ss(Line);
		std::string value;

		ss >> s >> tabcol >> s >> value;

		tabcol += '!';
		for (auto temp : tabcol)
		{
			if (temp == '.')
			{
				table = sf;
				sf = "";
				continue;
			}
			if (temp == '!')
			{

				column = sf;
				sf = "";
				break;
			}
			sf += temp;
		}

		if(lock_get(table) == "1")
			while(lock_get() == "1"){}
		else
			lock(table);

		std::string NewPath = "/home/kali/DataBase/DBfiles/" + abvg +"/" + table + "/" + "1.csv";
		std::filesystem::path temp_path;
		if (std::filesystem::exists(NewPath) && std::filesystem::is_regular_file(NewPath)) {
			temp_path = NewPath;
		}
		else {
			std::cerr << "Dir not found" << std::endl;
		}


		filePath = "/home/kali/DataBase/DBfiles/" + abvg +"/" + table;
	
		std::ifstream inFile(temp_path);
		std::fstream tempFile(filePath + "/temp.csv", std::ios::app);
		std::string line;
		int a = 0;
		while (std::getline(inFile, line))
		{
			tempFile << line << "\n";
		}
		tempFile.close();
		inFile.close();


		std::string delFile = temp_path.generic_string();
		remove(delFile.c_str());
		std::fstream inFile2(temp_path, std::ios_base::app);
		std::ifstream tempFile2(filePath + "/temp.csv");
		line = " ";
		a = 0;
		while (std::getline(tempFile2, line))
		{
			std::stringstream ss(line);
			std::string value1;
			std::stringstream ss2;
			int lineCheckd = 1;
			int currentColumn = 0;
			while (std::getline(ss, value1, ';')) {


				if (value1 == column)
				{
					a = 1;
				}

				if (currentColumn == temp2 && a == 1) {
					if (value1 == value)
					{
						lineCheckd = 0;
						break;
					}
				}
				if (a == 0)
					temp2++;

				currentColumn++;
			}
			if (lineCheckd == 0)
				continue;
			inFile2 << line << "\n";

		}
		tempFile2.close();
		inFile2.close();
		filePath += "/temp.csv";
		remove(filePath.c_str());

		unlock(table);
		
	}

	//--------------------------------------------------Разбор запроса(Рабочий, но не все команды)--------------------------------------------------------------
	void ParsingTheRequest(std::string Line) 
	{
		std::stringstream ss(Line);
		std::string part;

		ss >> part;


		if (part == "select")
		{
			Select(Line.erase(0, part.size() + 1));
		}
		else if (part == "insert") {
			ss >> part;
			InsertInto(ss);
		}
		else if (part == "delete")
			deleteFrom(Line);
		else
			std::cout << "\n\nInvalid query entered or query missing\n\n";


	}

	void Help()
	{
		std::cout << "\n\nBasic commands\n-select\n-from\n-where (operators and/or)\n-insert into\n-delete from\n-exit\n\n\n";
	}


public:
	void MainMenu()
	{
		std::string request;
		std::cout << "|-------------------------------SOFT.PRO.NSTU.MAX.PLUS.512TB.2CORES.2GB.GAMINGGPU.PAYME-----------------------|\n";
		Help();
		std::cout << "|-------------------------------SOFT.PRO.NSTU.MAX.PLUS.512TB.2CORES.2GB.GAMINGGPU.PAYME-----------------------|\n";
		CreatDataBase();
		// ScanAndOutputDirectory();

		while (request != "exit")
		{
			Tables = "";
			deth0 = '0';
			std::cout << "|-------------------------------SOFT.PRO.NSTU.MAX.PLUS.512TB.2CORES.2GB.GAMINGGPU.PAYME-----------------------|\n";
			std::cout << "\nMake a request: ";
			std::getline(std::cin, request);
			ParsingTheRequest(request);
			ClearCash();
		}
	}

};
int main()
{
	setlocale(LC_ALL, "rus");
	dataBase Main;

	//Main.Where();
	Main.MainMenu();
}

