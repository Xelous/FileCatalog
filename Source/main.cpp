#include <list>
#include <mutex>
#include <queue>
#include <chrono>
#include <atomic>
#include <thread>
#include <vector>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <filesystem>
#include <functional>

namespace xelous
{
	using namespace std::filesystem;	

	std::atomic<uint16_t> g_ActiveCount{ 0 };
	std::atomic<uint64_t> g_ListIndex{ 0 };
	std::mutex g_PathsLock;
	std::vector<std::list<path>> g_Paths;
	std::mutex g_DirectoriesLock;
	std::queue<path> g_Directories;

	class WorkerData
	{
	private:
		friend const uint16_t HouseKeeping();
		friend void WorkerFunction(const uint64_t& p_WorkerIndex, const path& p_Directory);

		std::atomic_bool m_Active;
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

			g_Workers.push_back(WorkerData(l_Thread));
		}
	}

	const uint16_t HouseKeeping()
	{
		uint16_t l_ActiveCount{ 0 };
		for (auto& l_Worker : g_Workers)
		{
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
}

int main(int p_argc, char* p_argv[])
{
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
}