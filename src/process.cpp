/*
*	The Kent C++CSP Library 
*	Copyright (C) 2002-2007 Neil Brown
*
*	This library is free software; you can redistribute it and/or
*	modify it under the terms of the GNU Lesser General Public
*	License as published by the Free Software Foundation; either
*	version 2.1 of the License, or (at your option) any later version.
*
*	This library is distributed in the hope that it will be useful,
*	but WITHOUT ANY WARRANTY; without even the implied warranty of
*	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
*	Lesser General Public License for more details.
*
*	You should have received a copy of the GNU Lesser General Public
*	License along with this library; if not, write to the Free Software
*	Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#include "cppcsp.h"
#include <iostream>
#include "thread_local.h"

#include <boost/lexical_cast.hpp>

using namespace csp;
using namespace csp::internal;
using namespace std;
using boost::lexical_cast;

namespace
{
//Doesn't currently support hot-pluggable CPUs
class NumCPUs
{
public:
	int num;
	inline operator int() {return num;}
	
#ifdef CPPCSP_WINDOWS
	inline NumCPUs()
	{
		SYSTEM_INFO info;
		GetSystemInfo(&info);
		num = info.dwNumberOfProcessors;
	}
#else
	inline NumCPUs()
	{
		//TODO later actually code this up under Linux
		num = 1;
	}
#endif

} numCPUs;


inline void _RunProcess(Process* gproc)
{
	try
	{		
		gproc->runProcess();
		gproc->endProcess();
	}
	catch (std::exception& e)
	{
		//what do you expect us to do about it if they haven't caught it?
		//just log it:

		std::cerr << "Uncaught exception from process: " << e.what() << std::endl;
	}
}
}

#ifdef CPPCSP_FIBERS

void CALLBACK Kernel::Fiber_Start(void* param)
{
	Process* process = (Process*)param;
	_RunProcess(process);
	internal::GetKernel()->data.stacksToDelete.push_back(make_pair(process,process->context));
	
	
	delete process;
	
	internal::GetKernel()->reschedule(NullProcessPtr);
	//Should never get here
	
}

#endif


#ifdef CPPCSP_CONTEXT

#if SIZEOF_INT == SIZEOF_VOIDP
void _WrapProcess(int param)
{
  Process* proc = reinterpret_cast<Process*>(param);
  Kernel::Context_Run(proc);
}
#elif 2*SIZEOF_INT == SIZEOF_VOIDP
void _WrapProcess(unsigned int hi, unsigned int lo)
{
  uintptr_t u = hi;
  u <<= 8 * SIZEOF_INT;
  u |= static_cast<uintptr_t>(lo);

  Process* proc = reinterpret_cast<Process*>(u);
  Kernel::Context_Run(proc);
}
#endif

void Kernel::Context_Run(Process* proc)
{
        _RunProcess(proc);    

	internal::GetKernel()->data.stacksToDelete.push_back(make_pair(proc,proc->stackPointer));
	delete proc;
	internal::GetKernel()->reschedule(NullProcessPtr);
}

void Kernel::Context_Create(Process* proc, stack_t stack)
{
  getcontext(&(proc->context));
  proc->context.uc_stack = stack;
  proc->context.uc_link = NULL;
  #if SIZEOF_INT == SIZEOF_VOIDP
  makecontext(&(proc->context), (void(*)()) _WrapProcess, 1, reinterpret_cast<int>(proc));
  #elif 2*SIZEOF_INT == SIZEOF_VOIDP
  uintptr_t u = reinterpret_cast<uintptr_t>(proc);
  unsigned int hi = static_cast<int>(u >> 8 * SIZEOF_INT);
  unsigned int lo = static_cast<int>(u & UINT_MAX);
  makecontext(&(proc->context), (void(*)()) _WrapProcess, 2, hi, lo);
  #else
    #error Unsupported integer/pointer size      
  #endif
	
}

#endif

void* PthreadFunc(void* p)
{
	Kernel::ThreadFunc(p);
	return NULL;
}

#ifdef CPPCSP_WINDOWS
DWORD WINAPI WinThreadFunc(void* p)
{
	Kernel::ThreadFunc(p);
	return 0;
}
#endif


	




namespace csp
{
	void Thread_Yield()
	{
	#ifdef CPPCSP_WINDOWS
		::Sleep(0);
	#else
		::sched_yield();
	#endif
	}

	void CPPCSP_Yield()
	{
		Kernel* kernel = internal::GetKernel();
		ProcessPtr current = kernel->currentProcess();		
		kernel->addProcessChain(current,current);
		kernel->reschedule();
	}
	
	/*
	CSProcess::CSProcess()
		:	Process(
				GetKernel(),
				GetCurrentThread(),
				65536)
	{
	}
	*/
	
	ThreadCSProcess::ThreadCSProcess(unsigned _stackSize)
		:	Process(
				GetKernel(),
				CurrentThreadId(),
				_stackSize)
	{
	}
#ifdef CPPCSP_NAMEDPROCESSES	
	ThreadCSProcess::ThreadCSProcess(const char* familyName,const char* processName)
		:	Process(
				GetKernel(),
				CurrentThreadId(),
				internal::GetThreadingConfig()->get(familyName,processName).second)
			//useKernelThread(internal::GetThreadingConfig()->get(familyName,processName).first == KernelThread)
	{
		//cerr << "Type, stacksize for " << familyName << " : " << processName << " is " << (useKernelThread ? "kernel" : "user") << ", " << stackSize << endl;
	}	
	
	ThreadCSProcess::ThreadCSProcess(const std::string& familyName,const std::string& processName)
		:	Process(
				GetKernel(),
				CurrentThreadId(),
				internal::GetThreadingConfig()->get(familyName,processName).second)
			//useKernelThread(internal::GetThreadingConfig()->get(familyName,processName).first == KernelThread)
	{
		//cerr << "Type, stacksize for " << familyName << " : " << processName << " is " << (useKernelThread ? "kernel" : "user") << ", " << stackSize << endl;
	}		
#endif //CPPCSP_NAMEDPROCESSES	
	
	void ThreadCSProcess::runProcess()
	{
		finalBarrier->enroll();
		run();
	}
	
	void ThreadCSProcess::endProcess()
	{	
		//cerr << "RESIGNING FROM FINAL BARRIER: " << this << endl;
		finalBarrier->resign();
	}
	

	void RunInThisThread(CSProcess* process)
	{
		Barrier barrier;
		ScopedBarrierEnd end(barrier.end());
		process->finalBarrier = barrier.enrolledEnd();
		try
		{
			//((ThreadCSProcess*)process)->start();
			process->startInThisThread();
		}
		catch (OutOfResourcesException&)
		{
			delete process;
			throw;
		}
		end.sync();
	}
	
	void Run(const ParallelHelper& helper)
	{
		Barrier barrier;
		ScopedBarrierEnd end(barrier.end());
		
		for (list<ThreadCSProcessPtr>::const_iterator it = helper.processList.begin();it != helper.processList.end();it++)
		{
			(*it)->finalBarrier = barrier.enrolledEnd();
		}

		ThreadCSProcess::StartAll(helper.processList.begin(),helper.processList.end());
		
		end.sync();		
	}
	
	void Run(const SequentialHelper& helper)
	{
		Barrier barrier;
		ScopedBarrierEnd end(barrier.end());
		
		for (list<ThreadCSProcessPtr>::const_iterator it = helper.processList.begin();it != helper.processList.end();it++)
		{
			(*it)->finalBarrier = barrier.enrolledEnd();
			try
			{
				(*it)->startInNewThread();
			}
			catch (OutOfResourcesException&)
			{
				while (it != helper.processList.end())
				{
					delete (*it);
					it++;
				}
				throw;
			}
			
			end.sync();
		}		
	}	
	
	void RunInThisThread(const ParallelHelperOneThread& helper)
	{
		Barrier barrier;
		ScopedBarrierEnd end(barrier.end());
		
		for (list<CSProcessPtr>::const_iterator it = helper.processList.begin();it != helper.processList.end();it++)
		{
			(*it)->finalBarrier = barrier.enrolledEnd();
		}

		CSProcess::StartAllInThisThread(helper.processList.begin(),helper.processList.end());
		
		end.sync();		
	}
	
	void RunInThisThread(const SequentialHelperOneThread& helper)
	{
		Barrier barrier;
		ScopedBarrierEnd end(barrier.end());
		
		for (list<CSProcessPtr>::const_iterator it = helper.processList.begin();it != helper.processList.end();it++)
		{
			(*it)->finalBarrier = barrier.enrolledEnd();
			try
			{
				(*it)->startInThisThread();
			}
			catch (OutOfResourcesException&)
			{
				while (it != helper.processList.end())
				{
					delete (*it);
					it++;
				}
				throw;
			}
			
			end.sync();
		}		
	}	
	
	void Run(const ParallelHelperOneThread& helper)
	{
		Run(helper.process());
	}
	
	void Run(const SequentialHelperOneThread& helper)
	{
		Run(helper.process());
	}	
	
	
	
	//TODO later make sure our processes are all deleted in the case of an out of resources error!	
	
	template <typename HELPER>
	class _HelperProcess : public CSProcess
	{
	private:
		const HELPER helper;
	protected:
		void run()
		{
			Run(helper);
		}
	public:
		inline _HelperProcess(const HELPER& _helper)
			:	CSProcess(65536),helper(_helper)
		{
		}
	};

	template <typename HELPER>
	class _HelperProcessThisThread : public CSProcess
	{
	private:
		const HELPER helper;
	protected:
		void run()
		{
			RunInThisThread(helper);
		}
	public:
		inline _HelperProcessThisThread(const HELPER& _helper)
			:	CSProcess(65536),helper(_helper)
		{
		}
	};

	
	CSProcessPtr ParallelHelper::process() const
	{	
		return new _HelperProcess<ParallelHelper>(*this);	
	}
	
	CSProcessPtr SequentialHelper::process() const
	{	
		return new _HelperProcess<SequentialHelper>(*this);	
	}
	
	CSProcessPtr ParallelHelperOneThread::process() const
	{	
		if (processList.size() == 1)
			return processList.front();
		else
			return new _HelperProcessThisThread<ParallelHelperOneThread>(*this);	
	}
	
	CSProcessPtr SequentialHelperOneThread::process() const
	{	
		if (processList.size() == 1)
			return processList.front();
		else
			return new _HelperProcessThisThread<SequentialHelperOneThread>(*this);
	}
	
	
	void Run(ThreadCSProcess* process)
	{
		Barrier barrier;
		ScopedBarrierEnd end(barrier.end());
		process->finalBarrier = barrier.enrolledEnd();
		try
		{
			process->startInNewThread();
		}
		catch (OutOfResourcesException&)
		{
			delete process;
			throw;
		}
		
		end.sync();
	}	
	
	void ScopedForking::fork(ThreadCSProcess* process)
	{
		process->finalBarrier = barrier.enrolledEnd();
		try
		{
			process->startInNewThread();
		}
		catch (OutOfResourcesException&)
		{
			delete process;
			throw;
		}
	}	

	void ScopedForking::forkInThisThread(CSProcess* process)
	{
		process->finalBarrier = barrier.enrolledEnd();
		try
		{
			process->startInThisThread();
		}
		catch (OutOfResourcesException&)
		{
			delete process;
			throw;
		}
	}
	
	void ScopedForking::forkInThisThread(const ParallelHelperOneThread& helper)
	{
		for (std::list<CSProcessPtr>::const_iterator it = helper.processList.begin();it != helper.processList.end();it++)
		{
			forkInThisThread(*it);
		}
	}
	
	void ScopedForking::forkInThisThread(const SequentialHelperOneThread& helper)
	{
		forkInThisThread(helper.process());
	}
	
	void ScopedForking::fork(const RunHelper& helper)
	{
		fork(helper.process());
	}
	
	void ScopedForking::fork(const ParallelHelper& helper)
	{
		for (std::list<ThreadCSProcessPtr>::const_iterator it = helper.processList.begin();it != helper.processList.end();it++)
		{
			fork(*it);
		}
	}
	
	namespace internal
	{
			Process* Primitive::currentProcess()
			{
			/*
			//We don't use GetFiberData because it will return non-NULL at the end of a process' lifetime, when we want it to return NULL
			
			#ifdef CPPCSP_FIBERS
				return (ProcessPtr) GetFiberData();
			#else
			*/
				return static_cast<Process*>(GetKernel()->currentProcess());
			//#endif
			}
			
			ThreadId Primitive::currentThread()
			{
				return CurrentThreadId();
			}
			
			ThreadId Primitive::getThreadId(Process* ptr)
			{
				return ptr->threadId;
			}
			
			void Primitive::freeProcessChain(ProcessPtr head, ProcessPtr tail)
			{
				//Processes can't move thread so:
				
				if (head != NullProcessPtr)
				{
					head->kernel->addProcessChain(head,tail);
				}
			}
			
			void Primitive::freeProcessNoAlt(ProcessPtr process)
			{
				if (process != NullProcessPtr)
				{
					//Just to be safe:
					process->nextProcess = NullProcessPtr;
				
					process->kernel->addProcessChain(process,process);
				}
			}		

			
			//Schedules another process but does not put this one back on the run queue
			void Primitive::reschedule()
			{
				GetKernel()->reschedule();
			}
			
			//Schedules another process and puts this one back on the run queue
			void Primitive::yield()
			{
				CPPCSP_Yield();				
			}
			
			//Spins.  On a 1-CPU system, this will yield (no point spinning when we are using the only CPU!)
			void Primitive::spin(int spinCount)
			{			
				if (numCPUs == 1)				
				{
					Thread_Yield();
				}
				else if (spinCount >= 3)
				{
					Thread_Yield();
				}
			}

		void ContextSwitch(Context* from,Context* to)
		{
		#ifdef CPPCSP_CONTEXT
			if (from != NULL)
			{	
                                swapcontext(from, to);		
			}
			else
			{
                                setcontext(to);
			}
		#endif
		
		#ifdef CPPCSP_FIBERS
			if (from == NULL || *from != *to)
			{
				SwitchToFiber(*to);
			}
		#endif
		}		
	
	} //namespace internal
		
		void ThreadCSProcess::startInNewThread()
		{			
			#ifndef WIN32
				pthread_t blah;
				//std::cerr << "Copying from kernel: " << GetKernel() << std::endl;
				Kernel* _kernel = new Kernel(GetKernel(),static_cast<internal::ProcessPtr>(this));
				kernel = _kernel;
				//std::cerr << "New kernel: " << this << std::endl;
				
				pthread_attr_t attr;
				pthread_attr_init(&attr);
				
				pthread_attr_setstacksize(&attr,stackSize);
				
				size_t guardsize;
				pthread_attr_getguardsize(&attr,&guardsize);
				
				//cerr << "Default guard size: " << guardsize << endl;
				
				if (0 != pthread_create(&blah,&attr,&PthreadFunc,_kernel))
				{
					pthread_attr_destroy(&attr);
					//cout << "THROWING OutOfResourcesError" << endl;
					throw OutOfResourcesException("Could not create pthread, stack size: " + lexical_cast<string>(stackSize));
				}
				
				pthread_attr_destroy(&attr);
			#else
				Kernel* _kernel = new Kernel(GetKernel(),(ProcessPtr)this);
				kernel = _kernel;
				
				#ifndef STACK_SIZE_PARAM_IS_A_RESERVATION
					//MinGW doesn't seem to define this value
					#define STACK_SIZE_PARAM_IS_A_RESERVATION 0x00010000
				#endif
				
				if (NULL == CreateThread(NULL,stackSize,&WinThreadFunc,_kernel,STACK_SIZE_PARAM_IS_A_RESERVATION,NULL))
				{
					//Try again without the STACK_SIZE_PARAM_IS_A_RESERVATION parameter, in case old flavours of Windows don't like it:
					if (NULL == CreateThread(NULL,stackSize,&WinThreadFunc,_kernel,0,NULL))
					{
						throw OutOfResourcesException("Could not create Windows Thread");
					}
				}
			#endif				
			
			
		}
		
		void CSProcess::startInThisThread()
		{	
			#ifdef CPPCSP_CONTEXT
				//cerr << "CREATING STACK OF SIZE: " << stackSize << " FOR " << this << endl;
                  
				stackPointer = new unsigned char[stackSize];
				
				if (stackPointer == NULL)
				{				
					throw OutOfResourcesException("Could not create new stack of size: " + lexical_cast<string>(stackSize));
				}
							
                                stack_t stack;
                                stack.ss_flags = 0;
                                stack.ss_sp = stackPointer;
                                stack.ss_size = stackSize;
                                Kernel::Context_Create(this, stack);

			#endif
			#ifdef CPPCSP_FIBERS
				/*	An Internet search suggested that if reserve size == commit size, both will be rounded up
				*	to the lowest multiple of 1MB (or the default stack size).  To avoid this, we add a page
				*	to the reserve size.				
				*/				
				context = CreateFiberEx(stackSize,stackSize+4096,0,Kernel::Fiber_Start,this);

				if (context == NULL)
				{
					char* buffer = new char[8192];
					FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM,NULL,GetLastError(),0,buffer,8192,NULL);
					std::string msg = buffer;
					delete []buffer;
					throw OutOfResourcesException("Error creating fiber: " + msg);
				}
			#endif				
				
				kernel = GetKernel();
				kernel->addProcessChain(this,this);
			
		}		
		
		void CSProcess::StartAllInThisThread(std::list<CSProcessPtr>::const_iterator begin, std::list<CSProcessPtr>::const_iterator end)
		{						
			try
			{
				while (begin != end)
				{
					(*begin)->startInThisThread();					
					begin++;
				}
			}
			catch (OutOfResourcesException&)
			{
				while (begin != end)
				{
					delete (*begin);
					begin++;
				}								
				
				throw;
			}			

		}
		
		void ThreadCSProcess::StartAll(std::list<ThreadCSProcessPtr>::const_iterator begin, std::list<ThreadCSProcessPtr>::const_iterator end)
		{						
			try
			{
				while (begin != end)
				{
					(*begin)->startInNewThread();					
					begin++;
				}
			}
			catch (OutOfResourcesException&)
			{
				while (begin != end)
				{
					delete (*begin);
					begin++;
				}								
				
				throw;
			}			

		}
		
	namespace internal
	{		
		TimeoutId Primitive::addTimeoutAlt(csp::Time* timeout,AltingProcessPtr proc) {return GetKernel()->getTimeoutQueue()->addTimeoutAlt(proc,timeout);}
			
		bool Primitive::removeTimeout(TimeoutId id) {return GetKernel()->getTimeoutQueue()->removeTimeout(id);}		
	}
		
}//namespace csp
