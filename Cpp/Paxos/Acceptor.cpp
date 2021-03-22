#include "Acceptor.h"

namespace paxos
{

Acceptor::Acceptor(void)
{
	m_maxSerialNum = 0;
	m_lastAcceptValue.serialNum = 0;
	m_lastAcceptValue.value = 0;
}

Acceptor::~Acceptor(void)
{
}

bool Acceptor::Propose(unsigned int serialNum, PROPOSAL &lastAcceptValue)
{
	if (0 == serialNum)
		return false;
	//提议不通过
	if (m_maxSerialNum > serialNum)
		return false;
	//接受提议
	//请完善下面逻辑

	/**********Begin**********/
	this->m_lastAcceptValue.serialNum = lastAcceptValue.serialNum;
	this->m_lastAcceptValue.value = lastAcceptValue.value;
	/**********End**********/

	return true;
}

bool Acceptor::Accept(PROPOSAL &value)
{
	if (0 == value.serialNum)
		return false;
	//Acceptor又重新答应了其他提议
	//请完善下面逻辑
	/**********Begin**********/
	if (value.serialNum >= m_lastAcceptValue.serialNum)
	{
		m_maxSerialNum = m_lastAcceptValue.serialNum;
		m_lastAcceptValue.serialNum = value.serialNum;
		m_lastAcceptValue.value = value.value;
	}
	/**********End**********/

	//批准提议通过
	//请完善下面逻辑
	/**********Begin**********/
	
	/**********End**********/

	return true;
}

} // namespace paxos
