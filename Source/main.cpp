#include <list>
#include <mutex>
#include <queue>
#include <chrono>
#include <atomic>
#include <thread>
#include <vector>
#include <sstream>
#include <cstdlib>
#include <cassert>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <filesystem>
#include <functional>
#include "../Contrib/blake2.h"

namespace xelous
{
	using namespace std::filesystem;	

	std::atomic<uint16_t> g_ActiveCount{ 0 };
	std::atomic<uint64_t> g_ListIndex{ 0 };
	std::mutex g_PathsLock;
	std::vector<std::list<path>> g_Paths;
	std::mutex g_DirectoriesLock;
	std::queue<path> g_Directories;

    using Hash = uint64_t;
    constexpr auto HashSize = sizeof(Hash);

    Hash Compute(const uint8_t* buffer, const uint64_t& bufferSize)
    {
        Hash result{ 0 };
        blake2b(reinterpret_cast<void*>(&result), HashSize, buffer, bufferSize, NULL, 0);
        return result;
    }

    class FileRecord
    {
    private:
        const path m_Path;        
        const uintmax_t m_FileSize;
        Hash m_Hash;

        friend void Report();
        friend void HashWorker(const path& p_Path, char* p_Buffer, size_t& p_BufferSize);

    public:
        FileRecord(const path& p_Path)
            :
            m_Path(p_Path),
            m_FileSize(exists(p_Path) ? file_size(p_Path) : 0),            
            m_Hash(0)
        {
        }

        const std::string ToXML() const 
        {
            std::ostringstream l_oss;
            l_oss << "<File><Path>" << m_Path.string() << "</Path><Size>" << m_FileSize << "</Size><Hash>" << m_Hash << "</Hash></File>";
            return l_oss.str();
        }
    };

    std::mutex g_RecordsLock;
    std::vector<FileRecord> g_Records;
    std::mutex g_EmptyFilesLock;
    std::vector<FileRecord> g_EmptyFiles;
    std::mutex g_FailedFilesLock;
    std::vector<FileRecord> g_FailedFiles;
    std::mutex g_SpecialLock;
    std::vector<FileRecord> g_SpecialFiles;

    void HashWorker(const path& p_Path, char* p_Buffer, size_t& p_BufferSize)
    {
        FileRecord l_Record(p_Path);

        if (l_Record.m_FileSize > 0)
        {
            if (p_Buffer)
            {
                if (p_BufferSize < l_Record.m_FileSize)
                {
                    p_Buffer = reinterpret_cast<char*>(realloc(reinterpret_cast<void*>(p_Buffer), p_BufferSize));                                        
                    p_BufferSize = l_Record.m_FileSize;
                }                
            }
            else
            {
                p_Buffer = reinterpret_cast<char*>(malloc(l_Record.m_FileSize));
                p_BufferSize = l_Record.m_FileSize;
            }

            std::memset(p_Buffer, 0, p_BufferSize);

            FILE* l_File = nullptr;
            fopen_s(&l_File, p_Path.string().c_str(), "r");
            if (l_File)
            {                
                const auto l_BytesRead = fread(reinterpret_cast<void*>(p_Buffer), 1, l_Record.m_FileSize, l_File);
                if (l_BytesRead > 0) // l_Record.m_FileSize)
                {
                    l_Record.m_Hash = Compute(reinterpret_cast<uint8_t*>(p_Buffer), l_Record.m_FileSize);                    
                }
                else
                {
                    std::scoped_lock<std::mutex> l_lock(g_SpecialLock);
                    g_SpecialFiles.push_back(l_Record);
                }

                fclose(l_File);

                {
                    std::scoped_lock<std::mutex> l_lock(g_RecordsLock);                    
                    g_Records.push_back(std::move(l_Record));                    
                }
            }
            else
            {
                std::scoped_lock<std::mutex> l_Lock(g_FailedFilesLock);
                g_FailedFiles.push_back(l_Record);
            }
        }
        else
        {
            std::scoped_lock<std::mutex> l_Lock(g_EmptyFilesLock);
            g_EmptyFiles.push_back(l_Record);
        }        
    }

	class WorkerData
	{
	private:
		friend const uint16_t HouseKeeping();
		friend void WorkerFunction(const uint64_t& p_WorkerIndex, const path& p_Directory);

		std::atomic_bool m_Active;
        std::mutex m_Lock;
		std::thread* m_WorkerThread;

	public:
		WorkerData(const WorkerData& p_Other)
			:
			m_Active(p_Other.m_Active.load()),
			m_WorkerThread(p_Other.m_WorkerThread)
		{

		}

		WorkerData(std::thread* p_Worker)
			:
			m_Active(true),
			m_WorkerThread(p_Worker)
		{
		}
	};

    std::mutex g_WorkerLock;
	std::vector<WorkerData> g_Workers;

	const bool AddDirectory(const path& p_Path)
	{
		if (exists(p_Path) && is_directory(p_Path))
		{
			std::scoped_lock<std::mutex> l_lock(g_DirectoriesLock);
			g_Directories.push(p_Path);
			return true;
		}
		return false;
	}

	void WorkerFunction(const uint64_t& p_WorkerIndex, const path& p_Directory)
	{	
		if (exists(p_Directory) && is_directory(p_Directory))
		{
			try
			{
				for (auto& l_iterator : directory_iterator(p_Directory))
				{
					path l_path(l_iterator);
					if (!AddDirectory(l_path))
					{
						try
						{							
							g_Paths[p_WorkerIndex].push_back(std::move(l_path));
						}
						catch (const std::exception& l_ex)
						{
							// likely error in the vector
							int gothere = 1;
						}
						catch (...)
						{
							int gothere = 1;
						}
					}
				}
			}
			catch (const std::exception& l_ex)
			{
				// Likely unaccessible folder, notable some of the hidden windows stuff
				// TODO - Log this somewhere else
			}
		}
        std::scoped_lock<std::mutex> l_WorkerLock(g_WorkerLock);
        std::scoped_lock<std::mutex> l_lock(g_Workers[p_WorkerIndex].m_Lock);
		g_Workers[p_WorkerIndex].m_Active = false;
	}

	void StartWorker()
	{
		path l_path;
		{
			std::scoped_lock<std::mutex> l_lock(g_DirectoriesLock);
			l_path = g_Directories.front();
			g_Directories.pop();
		}

		auto l_NextIndex = g_ListIndex.fetch_add(1);
		{
			std::scoped_lock<std::mutex> l_lock(g_DirectoriesLock);
			auto l_SizeCount = g_Paths.size();
			std::list<path> l_contents;
			//l_contents.resize(32);
			g_Paths.push_back(l_contents);
			assert(l_SizeCount == l_NextIndex);
			if (l_SizeCount != l_NextIndex)
			{
				throw "Error, creating worker out of index step";
			}

			std::thread* l_Thread = new std::thread(
				std::bind(
					&WorkerFunction,
					l_NextIndex,
					l_path));			

            {
                std::scoped_lock<std::mutex> l_WorkerLock(g_WorkerLock);
                g_Workers.push_back(WorkerData(l_Thread));
            }
		}
	}

	const uint16_t HouseKeeping()
	{
		uint16_t l_ActiveCount{ 0 };
		for (auto& l_Worker : g_Workers)
		{
            {
                std::scoped_lock<std::mutex> l_lock(l_Worker.m_Lock);
                if (!l_Worker.m_Active)
                {
                    if (l_Worker.m_WorkerThread)
                    {
                        if (l_Worker.m_WorkerThread->joinable())
                        {
                            l_Worker.m_WorkerThread->join();
                        }
                        delete l_Worker.m_WorkerThread;
                        l_Worker.m_WorkerThread = nullptr;
                    }
                }
                else
                {
                    ++l_ActiveCount;
                }
            }
		}
		return l_ActiveCount;
	}	

	void Process()
	{		
		g_Paths.reserve(4096);
		bool l_Active{ true };
		uint16_t l_ActiveCount{ 0 };
		do
		{
			{
				std::scoped_lock<std::mutex> l_lock(g_DirectoriesLock);
				l_Active = !g_Directories.empty();				
			}
			if (l_Active && l_ActiveCount < std::thread::hardware_concurrency() - 1)
			{
				StartWorker();
			}			

			l_ActiveCount = HouseKeeping();
			
			l_Active = (l_ActiveCount > 0);

			std::cout << "\r[" << l_ActiveCount << "] threads      "; //->Total Run : " << g_ListIndex << "           ";
		}
		while (l_Active);		
	}

    void Clean()
    {
		g_Paths.erase(std::remove_if(g_Paths.begin(), g_Paths.end(), [](std::list<path>& p_List) { return p_List.empty(); }));        
    }    

    void HashProcessing()
    {
        std::cout << "\r\nProcessing Hashes\r\n";        
                         
        auto l_LambdaInstance = []()
        {
            bool l_Continue{ false };

            char* l_Buffer = nullptr;
            size_t l_BufferSize = 0;
            std::list<path> l_Paths;

            do
            {
                {
                    std::scoped_lock<std::mutex> l_Lock(g_PathsLock);
                    l_Continue = !g_Paths.empty();
                    if (l_Continue)
                    {
                        l_Paths = g_Paths.front();
                        g_Paths.erase(g_Paths.begin(), g_Paths.begin() + 1);
                    }
                }

                if (l_Continue)
                {                                        
                    for (const auto& l_File : l_Paths)
                    {
                        HashWorker(l_File, l_Buffer, l_BufferSize);
                    }
                }
            }
            while (l_Continue);

            if (l_Buffer)
            {
                free(l_Buffer);
            }
        };

        std::vector<std::thread> l_Workers;
        for (unsigned int i = 0; i < std::thread::hardware_concurrency(); ++i)
        {
            l_Workers.emplace_back(std::thread(l_LambdaInstance));
        }        

        while (!l_Workers.empty())
        {            
            std::cout << "\rHashing Workers [" << l_Workers.size() << "]";
            for (auto l_Thread = l_Workers.begin(); l_Thread != l_Workers.end(); ++l_Thread)
            {
                if (l_Thread->joinable())
                {
                    l_Thread->join();
                    l_Workers.erase(l_Thread, l_Thread + 1);
                    break;
                }
            }
            std::cout << "\rHashing Workers [" << l_Workers.size() << "]";
        }
    }

    void Report()
    {
        std::cout << g_FailedFiles.size() << " Failed\r\n";
        std::cout << g_EmptyFiles.size() << " Empty\r\n";
        std::cout << g_Records.size() << " Recorded\r\n";

        for (const auto& l_File : g_Records)
        {
            std::string l_string = l_File.m_Path.string();
            if (l_string.length() > 25)
            {
                l_string = l_string.substr(l_string.length() - 25, 25);
            }
            if (l_File.m_Hash == 0)
            {
                int gothere = 1;
            }
            std::cout << std::setfill(' ') << std::setw(25) << l_string << "  " << std::setfill('0') << std::setw(20) << l_File.m_Hash << std::setfill(' ') << std::setw(20) << l_File.m_FileSize << "\r\n";
        }
    }

    void Output()
    {
        path l_ThePath("./output.xml");
        std::cout << "Outputting : " << l_ThePath.string() << std::endl;
        std::ofstream l_OutputFile(l_ThePath.string().c_str(), std::ios_base::out);
        if (l_OutputFile && l_OutputFile.is_open())
        {
            l_OutputFile << "<Files>\r\n";
            for (const auto& l_File : g_Records)
            {
                l_OutputFile << l_File.ToXML() << "\r\n";
            }
            l_OutputFile << "</Files>\r\n";

            l_OutputFile.close();
        }
    }

}

int main(int p_argc, char* p_argv[])
{
    auto l_Start = std::chrono::steady_clock::now();

	using namespace std::filesystem;
	using namespace xelous;

	for (int p_Index = 0; p_Index < p_argc; ++p_Index)
	{
		std::cout << p_argv[p_Index] << std::endl;

		path l_p{ p_argv[p_Index] };
		if (exists(l_p) && is_directory(l_p))
		{
			g_Directories.push(l_p);
		}
	}

	Process();

    Clean();

    HashProcessing();

    auto l_End = std::chrono::steady_clock::now();    

    //Report();

    Output();

    auto l_Duration = std::chrono::duration_cast<std::chrono::milliseconds>(l_End - l_Start);
    std::cout << "Processed [" << g_Records.size() << "] in " << l_Duration.count() << "ms : " << (l_Duration.count() / 1000) << "s\r\n";
}